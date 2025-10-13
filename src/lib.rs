use clap::Parser;
use csv::Writer;
use num_complex::Complex;
use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, DatapathTrait, Flow, Report};
use rustfft::FftPlanner;
use tracing::{debug, info};

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod bbr;
mod cubic;

static MEASUREMENT_INTERVAL: Duration = Duration::from_millis(10);
static FFT_APPROX_DURATION: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum CCModeArg {
    Cubic,
    Bbr,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "nimbus")]
pub struct NimbusConfig {
    #[arg(long = "ipc", default_value = "unix")]
    pub ipc: String,

    #[arg(long = "bw_est_mode")]
    pub bw_est_mode: bool,

    #[arg(long = "use_ewma")]
    pub use_ewma: bool,

    #[arg(long = "pulse_size", default_value = "0.25")]
    pub pulse_size: f64,

    #[arg(long = "frequency", default_value = "5.0")]
    pub frequency: f64,

    #[arg(long = "ccmode", value_enum, default_value = "cubic")]
    pub cc_mode: CCModeArg,

    #[arg(long = "uest", default_value = "12000000.0")]
    pub uest: f64,

    #[arg(long = "log_file")]
    pub log_file: Option<PathBuf>,

    #[arg(long = "spectrogram_log")]
    pub spectrogram_log: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Nimbus {
    cfg: NimbusConfig,
}

enum CCMode {
    Cubic(cubic::Cubic),
    Bbr(bbr::Bbr),
}

impl CCMode {
    fn curr_cwnd_bytes(&self) -> f64 {
        match self {
            CCMode::Cubic(c) => c.curr_cwnd_bytes(),
            CCMode::Bbr(b) => b.curr_cwnd_bytes(),
        }
    }
}

pub struct NimbusFlow<T: Ipc> {
    control_channel: Datapath<T>,
    sc: Scope,
    sock_id: u32,
    mss: u32,

    rtt: Duration,
    ewma_rtt: Option<f64>,
    last_update: Instant,
    base_rtt: f64,
    wait_time: Duration,
    start_time: Option<Instant>,

    uest: f64,
    rate: f64,

    fft: Arc<dyn rustfft::Fft<f64>>,
    fft_length: usize,
    frequency: f64,
    pulse_size: f64,
    zt_history: Vec<f64>,
    zt_lookback: usize,
    rtt_history: Vec<f64>,
    last_hist_update: Instant,

    rin_history: Vec<f64>,
    rout_history: Vec<f64>,
    bw_est_mode: bool,
    max_rout: f64,
    ewma_rin: f64,
    ewma_rout: f64,
    ewma_zt: f64,
    use_ewma: bool,
    cc_mode: CCMode,

    log_writer: Option<Writer<File>>,
    spectrogram_writer: Option<Writer<File>>,
}

impl From<NimbusConfig> for Nimbus {
    fn from(cfg: NimbusConfig) -> Self {
        Self { cfg }
    }
}

impl Nimbus {
    fn get_fft_length(&self) -> (usize, f64) {
        let t = MEASUREMENT_INTERVAL.as_secs_f64();
        // get next higher power of 2
        let n = (FFT_APPROX_DURATION.as_secs_f64() / t) as u32; // 5s / 10ms = 500
        let n = if n.count_ones() != 1 {
            1 << ((std::mem::size_of_val(&n) as u32 * 8) - n.leading_zeros())
        } else {
            n
        } as usize;
        let duration_of_fft = (n as f64) * t;
        return (n, duration_of_fft);
    }
}

impl<T: Ipc> CongAlg<T> for Nimbus {
    type Flow = NimbusFlow<T>;

    fn name() -> &'static str {
        "nimbus"
    }

    fn datapath_programs(&self) -> HashMap<&'static str, String> {
        std::iter::once((
            "nimbus_program",
            String::from(
                "
                (def
                    (Report
                        (volatile acked 0)
                        (volatile rtt 0)
                        (volatile loss 0)
                        (volatile rin 0)
                        (volatile rout 0)
                        (volatile timeout false)
                    )
                    (report_time 0)
                )
                (when true
                    (:= Report.acked (+ Report.acked Ack.bytes_acked))
                    (:= Report.rtt Flow.rtt_sample_us)
                    (:= Report.rin Flow.rate_outgoing)
                    (:= Report.rout Flow.rate_incoming)
                    (:= Report.loss Ack.lost_pkts_sample)
                    (:= Report.timeout Flow.was_timeout)
                    (fallthrough)
                )
                (when (|| Report.timeout (> Report.loss 0))
                    (report)
                    (:= Micros 0)
                )
                (when (> Micros report_time)
                    (report)
                    (:= Micros 0)
                )
            ",
            ),
        ))
        .collect()
    }

    fn new_flow(&self, control: Datapath<T>, info: DatapathInfo) -> Self::Flow {
        info!(
            ipc = ?self.cfg.ipc,
            sock_id = ?info.sock_id,
            bw_est_mode = ?self.cfg.bw_est_mode,
            use_ewma = ?self.cfg.use_ewma,
            pulse_size = ?self.cfg.pulse_size,
            frequency = ?self.cfg.frequency,
            uest = ?self.cfg.uest,
            "[nimbus] starting",
        );

        let (fft_length, fft_duration_secs) = self.get_fft_length();
        let mut planner = FftPlanner::new();
        let fft = planner.plan_fft_forward(fft_length);
        debug!(?fft_length, fft_duration_secs, "created fft instance");

        let now = Instant::now();
        let mut s = NimbusFlow {
            sock_id: info.sock_id,
            control_channel: control,
            sc: Default::default(),
            mss: info.mss,

            bw_est_mode: self.cfg.bw_est_mode, // default to true
            frequency: self.cfg.frequency,
            pulse_size: self.cfg.pulse_size,
            uest: self.cfg.uest,
            use_ewma: self.cfg.use_ewma,
            base_rtt: -0.001f64, // careful
            last_update: now,
            rtt: Duration::from_millis(300),
            ewma_rtt: None,
            start_time: None,

            rate: 100000f64,

            fft,
            fft_length,
            zt_history: vec![],
            zt_lookback: ((fft_duration_secs + 1.0) / MEASUREMENT_INTERVAL.as_secs_f64()) as usize,
            rtt_history: vec![],
            last_hist_update: now,

            rin_history: vec![],
            rout_history: vec![],
            max_rout: 0.0f64,
            ewma_rin: 0.0f64,
            ewma_rout: 0.0f64,
            ewma_zt: 0.0f64,

            wait_time: Duration::from_millis(5),

            cc_mode: match self.cfg.cc_mode {
                CCModeArg::Cubic => CCMode::Cubic(cubic::Cubic::new(info.mss)),
                CCModeArg::Bbr => CCMode::Bbr(bbr::Bbr::new(info.mss)),
            },

            log_writer: self
                .cfg
                .log_file
                .as_ref()
                .map(|f| Writer::from_path(f).unwrap()),

            spectrogram_writer: self
                .cfg
                .spectrogram_log
                .as_ref()
                .map(|f| Writer::from_path(f).unwrap()),
        };

        let wt = s.wait_time;
        s.sc = s.install(wt);
        s.send_pattern(s.rate, wt);
        s
    }
}

#[derive(serde::Serialize)]
struct NimbusRecord {
    id: u32,
    t_unix_ms: u128,
    since_start_ms: u64,
    uest_bps: f64,
    rin_bps: f64,
    rout_bps: f64,
    zt_bps: f64,
    rtt_us: u64,
    elasticity_max: f64,
    elasticity_sum: f64,
}

#[derive(serde::Serialize)]
struct SpectrogramRecord {
    id: u32,
    t_unix_ms: u128,
    since_start_ms: u64,
    frequency: f64,
    power: f64,
}

impl<T: Ipc> Flow for NimbusFlow<T> {
    fn on_report(&mut self, _sock_id: u32, m: Report) {
        let now = Instant::now();
        let (acked, rtt_us, mut rin, mut rout, loss, was_timeout) = self.get_fields(&m).unwrap();
        self.rtt = Duration::from_micros(rtt_us as _);

        match self.cc_mode {
            CCMode::Cubic(ref mut cubic) => {
                if loss > 0 {
                    cubic.drop(self.sock_id, self.rtt);
                    let cwnd = cubic.curr_cwnd_bytes();
                    self.rate = cwnd / self.rtt.as_secs_f64();
                    self.send_pattern(self.rate, self.wait_time);
                    return;
                }

                if was_timeout {
                    cubic.drop(self.sock_id, self.rtt); // Careful
                    return;
                }
            }
            _ => (),
        }

        let rtt_seconds = self.rtt.as_secs_f64();
        self.ewma_rtt = self
            .ewma_rtt
            .map(|ewma_rtt| 0.95 * ewma_rtt + 0.05 * rtt_seconds)
            .or(Some(rtt_seconds));
        if self.base_rtt <= 0.0 || rtt_seconds < self.base_rtt {
            // careful
            self.base_rtt = rtt_seconds;
        }

        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

        // CCA update
        match self.cc_mode {
            CCMode::Cubic(ref mut cubic) => {
                cubic.update_rate(acked as u64, self.rtt);
            }
            CCMode::Bbr(ref mut bbr) => {
                bbr.update(self.rtt, rout);
            }
        }

        // update self.rate
        self.rate = self.cc_mode.curr_cwnd_bytes() / rtt_seconds;

        let elapsed = (now - self.start_time.unwrap()).as_secs_f64();
        //let mut  float_rin = rin as f64;
        //let mut float_rout = rout as f64; // careful

        self.ewma_rin = 0.2 * rin + 0.8 * self.ewma_rin;
        self.ewma_rout = 0.2 * rout + 0.8 * self.ewma_rout;

        if self.use_ewma {
            rin = self.ewma_rin;
            rout = self.ewma_rout;
        }

        if self.max_rout < self.ewma_rout {
            self.max_rout = self.ewma_rout;
            if self.bw_est_mode {
                self.uest = self.max_rout;
            }
        }

        let mut zt = self.uest * (rin / rout) - rin;
        if zt.is_nan() {
            zt = 0.0;
        }
        self.ewma_zt = 0.2 * zt + 0.8 * self.ewma_zt;

        while now > self.last_hist_update {
            self.rin_history.push(rin);
            self.rout_history.push(rout);
            self.zt_history.push(zt);
            self.rtt_history.push(self.rtt.as_secs_f64());
            // some zt measurements might be missing.
            // so, we back-fill values given the sample we do have, as if we had gotten those
            // values regularly every `MEASUREMENT_INTERVAL`.
            self.last_hist_update += MEASUREMENT_INTERVAL;
        }

        // Overlay Nimbus pulse
        // check this
        self.rate = self.rate.max(0.05 * self.uest);
        self.rate = self.elasticity_est_pulse().max(0.05 * self.uest);

        self.send_pattern(self.rate, self.wait_time);
        self.measure_elasticity();
        self.last_update = Instant::now();

        debug!(
            ID = self.sock_id,
            base_rtt_sec = self.base_rtt,
            curr_rate_bps = self.rate * 8.0,
            newly_acked_bytes = acked,
            rin_bps = rin * 8.0,
            rout_bps = rout * 8.0,
            ewma_rin_bps = self.ewma_rin * 8.0,
            ewma_rout_bps = self.ewma_rout * 8.0,
            max_ewma_rout_bps = self.max_rout * 8.0,
            zt_bps = zt * 8.0,
            rtt_sec = rtt_seconds,
            uest_bps = self.uest * 8.0,
            elapsed = elapsed,
            "[nimbus] got ack"
        );
        //n.last_ack = m.Ack Careful
    }
}

impl<T: Ipc> NimbusFlow<T> {
    fn send_pattern(&self, mut rate: f64, _wait_time: Duration) {
        if self.start_time.is_none()
            || (Instant::now() - self.start_time.unwrap()) < Duration::from_secs(1)
        {
            rate = 2_000_000.0;
        }

        let win = (self.mss as f64).max(rate * 2.0 * self.rtt.as_secs_f64());
        self.control_channel
            .update_field(&self.sc, &[("Rate", rate as u32), ("Cwnd", win as u32)])
            .unwrap_or(());
    }

    fn install(&mut self, wait_time: Duration) -> Scope {
        self.control_channel
            .set_program(
                "nimbus_program",
                Some(&[("report_time", wait_time.as_micros() as u32)][..]),
            )
            .unwrap()
    }

    fn get_fields(&mut self, m: &Report) -> Option<(u32, u32, f64, f64, u32, bool)> {
        let sc = &self.sc;
        let acked = m
            .get_field("Report.acked", sc)
            .expect("expected acked field in returned measurement") as u32;
        let rtt = m
            .get_field("Report.rtt", sc)
            .expect("expected rtt field in returned measurement") as u32;
        let rin = m
            .get_field("Report.rin", sc)
            .expect("expected rin field in returned measurement") as f64;
        let rout = m
            .get_field("Report.rout", sc)
            .expect("expected rout field in returned measurement") as f64;
        let loss = m
            .get_field("Report.loss", sc)
            .expect("expected loss field in returned measurement") as u32;
        let was_timeout = m
            .get_field("Report.timeout", sc)
            .expect("expected timeout field in returned measurement")
            == 1;
        Some((acked, rtt, rin, rout, loss, was_timeout))
    }

    fn elasticity_est_pulse(&mut self) -> f64 {
        let elapsed = (Instant::now() - self.start_time.unwrap()).as_secs_f64();
        let fr_modified = self.uest;
        let mut phase = elapsed * self.frequency;
        phase -= phase.floor();
        let up_ratio = 0.25;
        if phase < up_ratio {
            self.rate
                + self.pulse_size
                    * fr_modified
                    * (2.0 * std::f64::consts::PI * phase * (0.5 / up_ratio)).sin()
        } else {
            self.rate
                + (up_ratio / (1.0 - up_ratio))
                    * self.pulse_size
                    * fr_modified
                    * (2.0
                        * std::f64::consts::PI
                        * (0.5 + (phase - up_ratio) * (0.5 / (1.0 - up_ratio))))
                        .sin()
        }
    }

    fn measure_elasticity(&mut self) {
        if self.start_time.is_none()
            || self.start_time.unwrap().elapsed() < Duration::from_secs(6)
            || self.zt_history.len() < self.zt_lookback as usize
        {
            return;
        }

        let end_index = self.zt_history.len() - 1;
        let start_index = self.zt_history.len().saturating_sub(self.zt_lookback);

        let raw_zt = &self.zt_history[start_index..end_index]; // careful: complexity
        let raw_rtt = &self.rtt_history[start_index..end_index];

        let mut clean_zt: Vec<Complex<f64>> = Vec::new(); // careful: complexity

        // if we got all measurements, the length will be n
        //   (which is round_to_next_power_of_2(duration_of_fft[5s] / measurement_interval[10ms])
        //   (so, usually 512)
        //
        // raw_rtt / measurement_interval is the number of measurements in that rtt. the rtt might
        // vary over time, so this way we can get a consistent number of zt measurements per rtt.
        for i in 0..self.fft_length {
            if i as usize >= raw_rtt.len() {
                return;
            }

            let j = i as usize
                + 2 * ((raw_rtt[i as usize] / MEASUREMENT_INTERVAL.as_secs_f64()) as usize);
            if j >= raw_zt.len() {
                return;
            }

            clean_zt.push(Complex::new(raw_zt[j], 0.0));
        }

        let mut fft_zt = self.detrend(clean_zt);
        self.fft.process(&mut fft_zt[..]);

        let mut freq: Vec<f64> = Vec::new();
        for i in 0..((self.fft_length / 2) as usize) {
            freq.push(
                i as f64 * (1.0 / (self.fft_length as f64 * MEASUREMENT_INTERVAL.as_secs_f64())),
            );
        }

        let elasticity = self.compute_elasticity(&freq, &fft_zt);
        let elasticity_sum = self.compute_elasticity_sum(&freq, &fft_zt);

        debug!(
            ID = self.sock_id,
            elapsed = ?self.start_time.unwrap().elapsed(),
            Elasticity = elasticity,
            SumBasedElasticity = elasticity_sum,
            Expected_Peak = self.frequency,
            "elasticity_inf"
        );

        let times = if self.log_writer.is_some() || self.spectrogram_writer.is_some() {
            let t_unix_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("unix time")
                .as_millis();
            let since_start_ms = self.start_time.unwrap().elapsed().as_millis() as u64;
            Some((t_unix_ms, since_start_ms))
        } else {
            None
        };

        if let Some(w) = &mut self.log_writer {
            let (t_unix_ms, since_start_ms) = times.unwrap();
            let r = NimbusRecord {
                id: self.sock_id,
                t_unix_ms,
                since_start_ms,
                uest_bps: self.uest * 8.0,
                rin_bps: self.ewma_rin * 8.0,
                rout_bps: self.ewma_rout * 8.0,
                zt_bps: self.ewma_zt * 8.0,
                rtt_us: self.rtt.as_micros() as u64,
                elasticity_max: elasticity,
                elasticity_sum,
            };

            let ok = w.serialize(r).map_err(anyhow::Error::new);
            if let Err(err) = ok.and_then(|_| w.flush().map_err(anyhow::Error::new)) {
                tracing::warn!(?err, "Could not write to csv file");
            }
        }

        if let Some(w) = &mut self.spectrogram_writer {
            let (t_unix_ms, since_start_ms) = times.unwrap();
            let mut ok = Ok(());
            for (power, f) in fft_zt
                .iter()
                .zip(freq.iter())
                // cut off freqs above 10Hz
                .take_while(|(_, f)| **f < 10.)
            {
                let s = SpectrogramRecord {
                    id: self.sock_id,
                    t_unix_ms,
                    since_start_ms,
                    frequency: *f,
                    power: power.norm(),
                };

                ok = ok.and_then(|_| w.serialize(s).map_err(anyhow::Error::new));
            }

            if let Err(err) = ok.and_then(|_| w.flush().map_err(anyhow::Error::new)) {
                tracing::warn!(?err, "Could not write to csv file");
            }
        }
    }

    fn compute_elasticity(&self, freq: &[f64], fft_zt: &[Complex<f64>]) -> f64 {
        let (zt_pulse_freqs_max_idx, _) = self.find_peak(
            self.frequency - 0.25,
            self.frequency + 0.25,
            &freq[..],
            &fft_zt[..],
        );
        let (zt_neighbor_freqs_max_idx, _) = self.find_peak(
            self.frequency + 0.5,
            2.0 * self.frequency - 0.5,
            &freq[..],
            &fft_zt[..],
        );

        return fft_zt[zt_pulse_freqs_max_idx].norm() / fft_zt[zt_neighbor_freqs_max_idx].norm();
    }

    fn find_peak(
        &self,
        start_freq: f64,
        end_freq: f64,
        xf: &[f64],
        fft: &[Complex<f64>],
    ) -> (usize, f64) {
        let mut max_ind = 0usize;
        let mut mean = 0.0;
        let mut count = 0.0f64;
        for j in 0..xf.len() {
            if xf[j] <= start_freq {
                max_ind = j;
                continue;
            }

            if xf[j] > end_freq {
                break;
            }

            mean += fft[j].norm();
            count += 1.0;
            if fft[j].norm() > fft[max_ind].norm() {
                max_ind = j;
            }
        }

        (max_ind, mean / count.max(1.0))
    }

    // `compute_elasticity()` finds the *max* value in the pulse region and divides it by the *max*
    // value in the neighbor region.
    //
    // Instead, we're going to divide the *totals*.
    fn compute_elasticity_sum(&self, freq: &[f64], fft_zt: &[Complex<f64>]) -> f64 {
        // pulse region: f_p +/- \epsilon = 0.5
        let pulse_region_start = self.frequency - 0.5;
        let pulse_region_end = self.frequency + 0.5;
        // neighbor region: frequencies from f_p + \epsilon to 2*f_p - \epsilon
        let neighbor_region_start = self.frequency + 0.5;
        let neighbor_region_end = (2. * self.frequency) + 0.5;
        let (pulse_region_sum, neighbor_region_sum) = freq
            .into_iter()
            .zip(fft_zt.into_iter())
            .skip_while(|(frq, _)| **frq < pulse_region_start)
            .fold((0.0, 0.0), |(pulse_sum, neighbor_sum), (frq, pwr)| {
                if *frq < pulse_region_end {
                    (pulse_sum + (pwr.norm()), neighbor_sum)
                } else if *frq > neighbor_region_start && *frq < neighbor_region_end {
                    (pulse_sum, neighbor_sum + (pwr.norm()))
                } else {
                    (pulse_sum, neighbor_sum)
                }
            });
        return pulse_region_sum / neighbor_region_sum;
    }

    fn mean_complex(&self, a: &[Complex<f64>]) -> f64 {
        let mean_val: f64 = a.iter().map(|x| x.re).sum();
        mean_val / (a.len() as f64)
    }

    fn detrend(&self, a: Vec<Complex<f64>>) -> Vec<Complex<f64>> {
        let mean_val = self.mean_complex(&a[..]);

        a.iter()
            .map(|x| Complex::new(x.re - mean_val, 0.0))
            .collect()
    }
}
