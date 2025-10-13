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

pub struct Bbr {
    probe_rtt_interval: Duration,
    bottle_rate: f64,
    min_rtt_us: Option<Duration>,
    min_rtt_timeout: Option<Instant>,
}

pub const PROBE_RTT_INTERVAL: Duration = Duration::from_secs(10);

impl Bbr {
    pub fn new(_mss: u32) -> Self {
        Self {
            probe_rtt_interval: PROBE_RTT_INTERVAL,
            bottle_rate: 1_000_000.0,
            min_rtt_us: None,
            min_rtt_timeout: None,
        }
    }

    pub fn curr_cwnd_bytes(&self) -> f64 {
        if let Some(minrtt) = self.min_rtt_us {
            self.bottle_rate * 2.0 * minrtt.as_secs_f64()
        } else {
            4.
        }
    }

    pub fn update(&mut self, rtt: Duration, rout_bps: f64) {
        let now = Instant::now();
        if self.min_rtt_timeout.is_none() {
            self.min_rtt_timeout = Some(now + self.probe_rtt_interval);
        }

        if self.min_rtt_us.is_none() {
            // exit probe_rtt mode
            debug!(?rtt, "switch to probe_bw mode");
            self.min_rtt_us = Some(rtt)
        }

        if rtt <= self.min_rtt_us.unwrap() {
            self.min_rtt_us = Some(rtt);
            self.min_rtt_timeout = Some(now + self.probe_rtt_interval);
        }

        if rout_bps > self.bottle_rate {
            self.bottle_rate = rout_bps;
        }

        if now > self.min_rtt_timeout.unwrap() {
            debug!("switch to probe_rtt mode");
            self.min_rtt_us = None;
            self.min_rtt_timeout = None;
        }
    }
}
