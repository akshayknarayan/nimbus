use csv::Writer;
use cubic::Cubic;
use num_complex::Complex;
use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, DatapathTrait, Flow, Report};
use rustfft::FftPlanner;
use structopt::StructOpt;
use tracing::{debug, info};

use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod cubic;

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "nimbus")]
pub struct NimbusConfig {
    #[structopt(long = "ipc", default_value = "unix")]
    pub ipc: String,

    #[structopt(long = "bw_est_mode")]
    pub bw_est_mode: bool,

    #[structopt(long = "use_ewma")]
    pub use_ewma: bool,

    #[structopt(long = "set_win_cap")]
    pub set_win_cap: bool,

    #[structopt(long = "delay_threshold", default_value = "1.25")]
    pub delay_threshold: f64,

    #[structopt(long = "init_delay_threshold", default_value = "1.25")]
    pub init_delay_threshold: f64,

    #[structopt(long = "pulse_size", default_value = "0.25")]
    pub pulse_size: f64,

    #[structopt(long = "frequency", default_value = "5.0")]
    pub frequency: f64,

    #[structopt(long = "uest", default_value = "12000000.0")]
    pub uest: f64,

    #[structopt(long = "log_file")]
    pub log_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct Nimbus {
    cfg: NimbusConfig,
}

impl From<NimbusConfig> for Nimbus {
    fn from(cfg: NimbusConfig) -> Self {
        Self { cfg }
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
            bw_est_mode = ?self.cfg.bw_est_mode ,
            use_ewma = ?self.cfg.use_ewma ,
            set_win_cap = ?self.cfg.set_win_cap ,
            delay_threshold = ?self.cfg.delay_threshold ,
            init_delay_threshold = ?self.cfg.init_delay_threshold ,
            pulse_size = ?self.cfg.pulse_size ,
            frequency = ?self.cfg.frequency ,
            uest = ?self.cfg.uest ,
            "[nimbus] starting",
        );

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
            //set_win_cap:  self.cfg.set_win_cap_arg,
            base_rtt: -0.001f64, // careful
            last_update: now,
            rtt: Duration::from_millis(300),
            ewma_rtt: None,
            start_time: None,

            rate: 100000f64,

            fft_planner: FftPlanner::new(),
            zout_history: vec![],
            zt_history: vec![],
            rtt_history: vec![],
            measurement_interval: Duration::from_millis(10),
            last_hist_update: now,
            ewma_elasticity: 1.0f64,
            ewma_alpha: 0.01f64,

            rin_history: vec![],
            rout_history: vec![],
            max_rout: 0.0f64,
            ewma_rin: 0.0f64,
            ewma_rout: 0.0f64,

            wait_time: Duration::from_millis(5),

            cubic: Cubic::new(info.mss),

            writer: self
                .cfg
                .log_file
                .as_ref()
                .map(|f| Writer::from_path(f).unwrap()),
        };

        //s.cubic_reset(); Careful
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
    rin_bps: f64,
    rout_bps: f64,
    zt_bps: f64,
    rtt_us: u64,
    elasticity: f64,
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

    fft_planner: rustfft::FftPlanner<f64>,
    frequency: f64,
    pulse_size: f64,
    zout_history: Vec<f64>,
    zt_history: Vec<f64>,
    rtt_history: Vec<f64>,
    measurement_interval: Duration,
    last_hist_update: Instant,
    //switching_thresh: f64,
    ewma_elasticity: f64,
    ewma_alpha: f64,

    rin_history: Vec<f64>,
    rout_history: Vec<f64>,
    bw_est_mode: bool,
    max_rout: f64,
    ewma_rin: f64,
    ewma_rout: f64,
    use_ewma: bool,
    //set_win_cap: bool,
    cubic: cubic::Cubic,

    writer: Option<Writer<File>>,
}

impl<T: Ipc> Flow for NimbusFlow<T> {
    fn on_report(&mut self, _sock_id: u32, m: Report) {
        let now = Instant::now();
        let (acked, rtt_us, mut rin, mut rout, loss, was_timeout) = self.get_fields(&m).unwrap();
        self.rtt = Duration::from_micros(rtt_us as _);

        if loss > 0 {
            self.cubic.drop(self.sock_id, self.rtt);
            let cwnd = self.cubic.curr_cwnd_bytes();
            self.rate = cwnd / self.rtt.as_secs_f64();
            self.send_pattern(self.rate, self.wait_time);
            return;
        }

        if was_timeout {
            self.cubic.drop(self.sock_id, self.rtt); // Careful
            return;
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

        // Cubic update
        self.cubic.update_rate(acked as u64, self.rtt);
        // update self.rate based on cubic
        self.rate = self.cubic.curr_cwnd_bytes() / rtt_seconds;

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

        while now > self.last_hist_update {
            self.rin_history.push(rin);
            self.rout_history.push(rout);
            self.zout_history.push(self.uest - rout);
            self.zt_history.push(zt);
            self.rtt_history.push(self.rtt.as_secs_f64());
            self.last_hist_update += self.measurement_interval;
        }

        // Overlay Nimbus pulse
        // check this
        self.frequency = 5.0f64;
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
        let mut duration_of_fft = 5.0;
        let t = self.measurement_interval.as_secs_f64();

        // get next higher power of 2
        let n = (duration_of_fft / t) as i32; // 5s / 10ms = 500
        let n = if n.count_ones() != 1 {
            1 << (32 - n.leading_zeros())
        } else {
            n
        };

        duration_of_fft = (n as f64) * t;

        if self.start_time.is_none()
            || self.start_time.unwrap().elapsed() < Duration::from_secs(6)
            || self.zt_history.len() < n as usize
        {
            return;
        }

        let end_index = self.zt_history.len() - 1;
        let start_index = self
            .zt_history
            .len()
            .saturating_sub(((duration_of_fft + 1.0) / t) as usize);

        let raw_zt = &self.zt_history.clone()[start_index..end_index]; // careful: complexity
        let raw_rtt = &self.rtt_history.clone()[start_index..end_index];
        let raw_zout = &self.zout_history.clone()[start_index..end_index];

        let mut clean_zt: Vec<Complex<f64>> = Vec::new(); // careful: complexity
        let mut clean_zout: Vec<Complex<f64>> = Vec::new();
        let mut clean_rtt: Vec<Complex<f64>> = Vec::new();

        for i in 0..n {
            if i as usize >= raw_rtt.len() {
                return;
            }

            let j = i as usize + 2 * ((raw_rtt[i as usize] / t) as usize);
            if j >= raw_zt.len() {
                return;
            }

            clean_zt.push(Complex::new(raw_zt[j], 0.0));
            clean_zout.push(Complex::new(raw_zout[i as usize], 0.0));
            clean_rtt.push(Complex::new(raw_rtt[i as usize], 0.0));
        }

        //let avg_rtt = Duration::from_millis(
        //    (1e3 * self.mean_complex(&clean_rtt[(0.75 * (clean_rtt.len() as f32)) as usize..]))
        //        as u64,
        //);
        let avg_zt = self.mean_complex(&clean_zt[(0.75 * (clean_zt.len() as f32)) as usize..]);

        let mut fft_zt = self.detrend(clean_zt);
        let fft_zt_temp = self.fft_planner.plan_fft_forward(fft_zt.len());
        fft_zt_temp.process(&mut fft_zt[..]);

        let mut fft_zout = self.detrend(clean_zout);
        let fft_zout_temp = self.fft_planner.plan_fft_forward(fft_zout.len());
        fft_zout_temp.process(&mut fft_zout[..]);

        let mut freq: Vec<f64> = Vec::new();
        for i in 0..((n / 2) as usize) {
            freq.push(i as f64 * (1.0 / (n as f64 * t)));
        }

        let expected_peak = self.frequency;

        if avg_zt < 0.1 * self.uest {
            self.ewma_elasticity = 0.0;
        } else if avg_zt > 0.9 * self.uest {
            self.ewma_elasticity =
                (1.0 - self.ewma_alpha) * self.ewma_elasticity + self.ewma_alpha * 6.0;
        }

        let (_, mean_zt) = self.find_peak(
            2.2 * expected_peak,
            3.8 * expected_peak,
            &freq[..],
            &fft_zt[..],
        );
        let (exp_peak_zt, _) = self.find_peak(
            expected_peak - 0.5,
            expected_peak + 0.5,
            &freq[..],
            &fft_zt[..],
        );
        let (exp_peak_zout, _) = self.find_peak(
            expected_peak - 0.5,
            expected_peak + 0.5,
            &freq[..],
            &fft_zout[..],
        );
        let (other_peak_zt, _) = self.find_peak(
            expected_peak + 1.5,
            2.0 * expected_peak - 0.5,
            &freq[..],
            &fft_zt[..],
        );
        let (other_peak_zout, _) = self.find_peak(
            expected_peak + 1.5,
            2.0 * expected_peak - 0.5,
            &freq[..],
            &fft_zout[..],
        );
        let mut elasticity2 = fft_zt[exp_peak_zt].norm() / fft_zt[other_peak_zt].norm();
        let elasticity = (fft_zt[exp_peak_zt].norm() - mean_zt) / fft_zout[exp_peak_zout].norm();
        if fft_zt[exp_peak_zt].norm() < 0.25 * fft_zout[exp_peak_zout].norm() {
            elasticity2 = elasticity2.min(3.0);
            elasticity2 *=
                ((fft_zt[exp_peak_zt].norm() / fft_zout[exp_peak_zout].norm()) / 0.25).min(1.0);
        }
        self.ewma_elasticity =
            (1.0 - self.ewma_alpha) * self.ewma_elasticity + self.ewma_alpha * elasticity2;

        if (fft_zout[exp_peak_zout].norm() / fft_zout[other_peak_zout].norm()) < 2.0 {
            self.ewma_elasticity =
                (1.0 - self.ewma_alpha) * self.ewma_elasticity + self.ewma_alpha * 3.0;
        }

        debug!(
            ID = self.sock_id,
            Zout_peak_val = fft_zout[exp_peak_zout].norm(),
            Zt_peak_val = fft_zt[exp_peak_zt].norm(),
            elapsed = ?self.start_time.unwrap().elapsed(),
            Elasticity = elasticity,
            Elasticity2 = elasticity2,
            EWMAElasticity = self.ewma_elasticity,
            Expected_Peak = expected_peak,
            "elasticity_inf"
        );
        if let Some(w) = &mut self.writer {
            let r = NimbusRecord {
                id: self.sock_id,
                t_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("unix time")
                    .as_millis(),
                since_start_ms: self.start_time.unwrap().elapsed().as_millis() as u64,
                rin_bps: self.ewma_rin * 8.0,
                rout_bps: self.ewma_rout * 8.0,
                zt_bps: avg_zt * 8.0,
                rtt_us: self.rtt.as_micros() as u64,
                elasticity: elasticity2,
            };
            w.serialize(r).unwrap();
            w.flush().unwrap();
        }
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
