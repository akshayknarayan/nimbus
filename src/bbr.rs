//! An adaptation of BBR as the underlying CCA to use with Nimbus.
//!
//! Nimbus is already going to implement pulsing for us, so we don't have to use BBR's pulsing
//! separately. Instead, our main goal here is to update the bottleneck bandwidth estimate and
//! implement the PROBE_BW / PROBE_RTT mode switching.
//!
//! We also don't need to bother with the traffic policer detection logic since with the elasticity
//! detector, that happens separately.
//!
//! We end up with a quite simple distillation of the BBR logic:
//! ```text
//! self.bottle_rate = max(bottle_rate, r_out)
//! self.min_rtt = min(min_rtt, rtt)
//! if !probe_rtt_timeout() {
//!   self.cwnd = 2 * self.bottle_rate * self.min_rtt
//! } else {
//!   self.cwnd = 4
//! }
//! ```

use std::{
    time::{Duration, Instant},
    u32,
};
use tracing::debug;

const PROBE_RTT_CWND_PKTS: u32 = 4;

pub struct Bbr {
    probe_rtt_interval: Duration,
    bottle_rate: f64,
    bottle_rate_epoch: Option<Instant>,
    bottle_rate_epoch_max: f64,
    bottle_rate_epoch_dur: Duration,
    mss: u32,
    min_rtt_us: Option<Duration>,
    min_rtt_timeout: Option<Instant>,
}

pub const PROBE_RTT_INTERVAL: Duration = Duration::from_secs(10);

impl Bbr {
    pub fn new(mss: u32, nimbus_pulse_freq: f64) -> Self {
        Self {
            probe_rtt_interval: PROBE_RTT_INTERVAL,
            bottle_rate: 1_000_000.0,
            bottle_rate_epoch: None,
            bottle_rate_epoch_max: 0.,
            bottle_rate_epoch_dur: Duration::from_secs_f64(1. / nimbus_pulse_freq),
            min_rtt_us: None,
            min_rtt_timeout: None,
            mss,
        }
    }

    pub fn curr_cwnd_bytes(&self) -> f64 {
        if let Some(minrtt) = self.min_rtt_us {
            self.bottle_rate * 1.5 * minrtt.as_secs_f64()
        } else {
            (PROBE_RTT_CWND_PKTS as f64) * (self.mss as f64)
        }
    }

    pub fn update(&mut self, rtt: Duration, rout_bps: f64) {
        let now = Instant::now();
        if self.min_rtt_timeout.is_none() {
            self.min_rtt_timeout = Some(now + self.probe_rtt_interval);
        }

        if self.bottle_rate_epoch.is_none() {
            self.bottle_rate_epoch = Some(now);
        }

        // when in probe_rtt mode, exit back to probe_bw with a new min_rtt value once rout drops
        // low enough.
        //
        // how low is low enough?
        // rate_bytes_per_sec = (4 (packets) * mss) bytes / curr-rtt (sec)
        // wait for rout_bits_per_sec to drop to within a factor of 2 of this value.
        if self.min_rtt_us.is_none() {
            if rout_bps
                <= 2. * (8. * (PROBE_RTT_CWND_PKTS as f64) * (self.mss as f64) / rtt.as_secs_f64())
            {
                debug!(?self.bottle_rate, ?rtt, "switch to probe_bw mode");
                self.min_rtt_us = Some(rtt);
            } else {
                debug!(?rtt, ?rout_bps, "staying in probe_rtt mode");
                return;
            }
        }

        if rtt <= self.min_rtt_us.unwrap() {
            self.min_rtt_us = Some(rtt);
            // if we observed a lower min_rtt, reset the probe_rtt timer
            self.min_rtt_timeout = Some(now + self.probe_rtt_interval);
        }

        if now > self.min_rtt_timeout.unwrap() {
            debug!(?self.bottle_rate, ?self.min_rtt_us, "switch to probe_rtt mode");
            self.min_rtt_us = None;
            self.min_rtt_timeout = None;
            self.bottle_rate_epoch = None;
            self.bottle_rate_epoch_max = 0.;
            return;
        }

        if rout_bps > self.bottle_rate_epoch_max {
            self.bottle_rate_epoch_max = rout_bps;
        }

        // in BBR's original implementation, `bottle_rate` is a max over 8 RTTs: 1 RTT "up", 1 RTT
        // "down", and 6 RTTs "cruise".
        //
        // Our pulsing is with Nimbus now, which is continuous (no "cruise"). So, do our windowing
        // over a single Nimbus pulse (self.bottle_rate_epoch_dur = 1 / nimbus-pulse-freq).
        if (now - self.bottle_rate_epoch.unwrap()) > self.bottle_rate_epoch_dur {
            // set bottle_rate (used to determine cwnd) to the max over the current epoch
            self.bottle_rate = self.bottle_rate_epoch_max;
            // reset windowed epoch
            self.bottle_rate_epoch_max = 0.;
            self.bottle_rate_epoch = Some(Instant::now());
        }
    }
}
