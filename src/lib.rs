use csv::Writer;
use num_complex::Complex;
use portus::ipc::Ipc;
use portus::lang::Scope;
use portus::{CongAlg, Datapath, DatapathInfo, DatapathTrait, Flow, Report};
use rustfft::FftPlanner;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tracing::{debug, info};

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

    #[structopt(long = "xtcp_flows", default_value = "2")]
    pub xtcp_flows: usize,

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
            xtcp_flows = ?self.cfg.xtcp_flows,
            "[nimbus] starting",
        );

        let now = Instant::now();

        let mut s = NimbusFlow {
            sock_id: info.sock_id,
            control_channel: control,
            sc: Default::default(),
            mss: info.mss,

            bw_est_mode: self.cfg.bw_est_mode, // default to true
            xtcp_flows: self.cfg.xtcp_flows as i32,
            frequency: self.cfg.frequency,
            pulse_size: self.cfg.pulse_size,
            uest: self.cfg.uest,
            use_ewma: self.cfg.use_ewma,
            //set_win_cap:  self.cfg.set_win_cap_arg,
            base_rtt: -0.001f64, // careful
            last_drop: vec![],
            last_update: now,
            rtt: Duration::from_millis(300),
            ewma_rtt: 0.1f64,
            start_time: None,
            ssthresh: vec![],
            cwnd_clamp: 2e6 * 1448.0,

            rate: 100000f64,
            ewma_rate: 10000f64,
            cwnd: vec![],

            zout_history: vec![],
            zt_history: vec![],
            rtt_history: vec![],
            measurement_interval: Duration::from_millis(10),
            last_hist_update: now,
            ewma_elasticity: 1.0f64,
            ewma_alpha: 0.01f64,

            rin_history: vec![],
            rout_history: vec![],
            agg_last_drop: now,
            max_rout: 0.0f64,
            ewma_rin: 0.0f64,
            ewma_rout: 0.0f64,

            wait_time: Duration::from_millis(5),

            //cubic_init_cwnd: 10f64,
            cubic_cwnd: 10f64,
            cubic_ssthresh: ((0x7fffffff as f64) / 1448.0),
            cwnd_cnt: 0f64,
            tcp_friendliness: true,
            cubic_beta: 0.3f64,
            fast_convergence: true,
            c: 0.4f64,

            wlast_max: 0f64,
            epoch_start: None,
            origin_point: 0f64,
            d_min: -0.0001f64,
            wtcp: 0f64,
            k: 0f64,
            ack_cnt: 0f64,
            cnt: 0f64,
            writer: self
                .cfg
                .log_file
                .as_ref()
                .map(|f| Writer::from_path(f).unwrap()),
        };

        s.cwnd = (0..s.xtcp_flows)
            .map(|_| s.rate / (s.xtcp_flows as f64))
            .collect();
        s.last_drop = (0..s.xtcp_flows).map(|_| now).collect();
        s.ssthresh = (0..s.xtcp_flows).map(|_| s.cwnd_clamp).collect();

        //s.cubic_reset(); Careful
        let wt = s.wait_time;
        s.sc = s.install(wt);
        s.send_pattern(s.rate, wt);
        if let Some(w) = &mut s.writer {
            w.write_record(["id", "duration", "elasticity"]).unwrap();
        }

        s
    }
}

pub struct NimbusFlow<T: Ipc> {
    control_channel: Datapath<T>,
    sc: Scope,
    sock_id: u32,
    mss: u32,

    rtt: Duration,
    ewma_rtt: f64,
    last_drop: Vec<Instant>,
    last_update: Instant,
    base_rtt: f64,
    wait_time: Duration,
    start_time: Option<Instant>,

    uest: f64,
    rate: f64,
    ewma_rate: f64,
    cwnd: Vec<f64>,
    xtcp_flows: i32,
    ssthresh: Vec<f64>,
    cwnd_clamp: f64,

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
    agg_last_drop: Instant,
    bw_est_mode: bool,
    max_rout: f64,
    ewma_rin: f64,
    ewma_rout: f64,
    use_ewma: bool,
    //set_win_cap: bool,

    //cubic_init_cwnd: f64,
    cubic_cwnd: f64,
    cubic_ssthresh: f64,
    cwnd_cnt: f64,
    tcp_friendliness: bool,
    cubic_beta: f64,
    fast_convergence: bool,
    c: f64,
    wlast_max: f64,
    epoch_start: Option<Instant>,
    origin_point: f64,
    d_min: f64,
    wtcp: f64,
    k: f64,
    ack_cnt: f64,
    cnt: f64,

    writer: Option<Writer<File>>,
}

impl<T: Ipc> Flow for NimbusFlow<T> {
    fn on_report(&mut self, _sock_id: u32, m: Report) {
        let now = Instant::now();
        let (acked, rtt_us, mut rin, mut rout, loss, was_timeout) = self.get_fields(&m).unwrap();
        self.rtt = Duration::from_micros(rtt_us as _);

        if loss > 0 {
            self.handle_drop();
            return;
        }

        if was_timeout {
            self.handle_timeout(); // Careful
            return;
        }

        let rtt_seconds = self.rtt.as_secs_f64();
        self.ewma_rtt = 0.95 * self.ewma_rtt + 0.05 * rtt_seconds;
        if self.base_rtt <= 0.0 || rtt_seconds < self.base_rtt {
            // careful
            self.base_rtt = rtt_seconds;
        }

        if self.start_time.is_none() {
            self.start_time = Some(now);
        }

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

        // check this
        self.frequency = 5.0f64;
        self.update_rate_loss(acked as u64);

        self.rate = self.rate.max(0.05 * self.uest);
        self.rate = self.elasticity_est_pulse().max(0.05 * self.uest);

        self.send_pattern(self.rate, self.wait_time);
        self.should_switch_flow_mode();
        self.last_update = Instant::now();

        debug!(
            ID = self.sock_id,
            base_rtt = self.base_rtt,
            curr_rate = self.rate * 8.0,
            curr_cwnd = self.cwnd[0],
            newly_acked = acked,
            rin = rin * 8.0,
            rout = rout * 8.0,
            ewma_rin = self.ewma_rin * 8.0,
            ewma_rout = self.ewma_rout * 8.0,
            max_ewma_rout = self.max_rout * 8.0,
            zt = zt * 8.0,
            rtt = rtt_seconds,
            uest = self.uest * 8.0,
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

    fn handle_drop(&mut self) {
        self.cubic_drop()
    }

    fn cubic_drop(&mut self) {
        let now = Instant::now();
        if (now - self.last_drop[0]) < self.rtt {
            return;
        }
        self.epoch_start = None; //careful
        if (self.cubic_cwnd < self.wlast_max) && self.fast_convergence {
            self.wlast_max = self.cubic_cwnd * ((2.0 - self.cubic_beta) / 2.0);
        } else {
            self.wlast_max = self.cubic_cwnd;
        }
        self.cubic_cwnd *= 1.0 - self.cubic_beta;
        self.cubic_ssthresh = self.cubic_cwnd;
        self.cwnd[0] = self.cubic_cwnd * 1448.0;
        self.rate = self.cwnd[0] / self.rtt.as_secs_f64();
        self.send_pattern(self.rate, self.wait_time);

        debug!(
            ID = self.sock_id,
            time_since_last_drop = (now - self.last_drop[0]).as_secs_f64(),
            rtt = ?self.rtt,
            "[nimbus cubic] got drop"
        );
        self.last_drop[0] = now;
        self.agg_last_drop = now;
    }

    fn update_rate_loss(&mut self, new_bytes_acked: u64) {
        self.update_rate_cubic(new_bytes_acked)
    }

    fn update_rate_cubic(&mut self, new_bytes_acked: u64) {
        let mut no_of_acks = (new_bytes_acked as f64) / self.mss as f64;
        if self.cubic_cwnd < self.cubic_ssthresh {
            if (self.cubic_cwnd + no_of_acks) < self.cubic_ssthresh {
                self.cubic_cwnd += no_of_acks;
                no_of_acks = 0.0;
            } else {
                no_of_acks -= self.cubic_ssthresh - self.cubic_cwnd;
                self.cubic_cwnd = self.cubic_ssthresh;
            }
        }
        let rtt_seconds = self.rtt.as_secs_f64();
        for _ in 0..no_of_acks as usize {
            if self.d_min <= 0.0 || rtt_seconds < self.d_min {
                self.d_min = rtt_seconds;
            }
            self.cubic_update();
            if self.cwnd_cnt > self.cnt {
                self.cubic_cwnd += 1.0;
                self.cwnd_cnt = 0.0;
            } else {
                self.cwnd_cnt += 1.0;
            }
        }
        self.cwnd[0] = self.cubic_cwnd * 1448.0;
        let total_cwnd = self.cwnd[0];
        self.rate = total_cwnd / rtt_seconds;
        self.ewma_rate = self.rate;
    }

    fn cubic_update(&mut self) {
        let now = Instant::now();
        self.ack_cnt += 1.0;
        if self.epoch_start.is_none() {
            self.epoch_start = Some(now);
            if self.cubic_cwnd < self.wlast_max {
                self.k = (0.0f64.max((self.wlast_max - self.cubic_cwnd) / self.c)).powf(1.0 / 3.0);
                self.origin_point = self.wlast_max;
            } else {
                self.k = 0.0;
                self.origin_point = self.cubic_cwnd;
            }
            self.ack_cnt = 1.0;
            self.wtcp = self.cubic_cwnd;
        }
        let t =
            (now + Duration::from_secs_f64(self.d_min) - self.epoch_start.unwrap()).as_secs_f64();
        let target = self.origin_point + self.c * ((t - self.k) * (t - self.k) * (t - self.k));
        if target > self.cubic_cwnd {
            self.cnt = self.cubic_cwnd / (target - self.cubic_cwnd);
        } else {
            self.cnt = 100.0 * self.cubic_cwnd;
        }
        if self.tcp_friendliness {
            self.cubic_tcp_friendliness();
        }
    }

    fn cubic_tcp_friendliness(&mut self) {
        self.wtcp +=
            ((3.0 * self.cubic_beta) / (2.0 - self.cubic_beta)) * (self.ack_cnt / self.cubic_cwnd);
        self.ack_cnt = 0.0;
        if self.wtcp > self.cubic_cwnd {
            let max_cnt = self.cubic_cwnd / (self.wtcp - self.cubic_cwnd);
            if self.cnt > max_cnt {
                self.cnt = max_cnt;
            }
        }
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

    fn should_switch_flow_mode(&mut self) {
        let mut duration_of_fft = 5.0;
        let t = self.measurement_interval.as_secs_f64();

        // get next higher power of 2
        let n = (duration_of_fft / t) as i32;
        let n = if n.count_ones() != 1 {
            1 << (32 - n.leading_zeros())
        } else {
            n
        };

        duration_of_fft = (n as f64) * t;

        if self.start_time.is_none() || self.start_time.unwrap().elapsed() < Duration::from_secs(10)
        {
            return;
        }

        let end_index = self.zt_history.len() - 1;
        let start_index = self.zt_history.len() - ((duration_of_fft + 1.0) / t) as usize;

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
        let mut fft_zt_temp_plan = FftPlanner::new();
        let fft_zt_temp = fft_zt_temp_plan.plan_fft_forward(fft_zt.len());
        fft_zt_temp.process(&mut fft_zt[..]);

        let mut fft_zout = self.detrend(clean_zout);
        let mut fft_zout_temp_plan = FftPlanner::new();
        let fft_zout_temp = fft_zout_temp_plan.plan_fft_forward(fft_zout.len());
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
        let duration = self.start_time.unwrap().elapsed().as_micros();
        if let Some(w) = &mut self.writer {
            w.write_record([
                self.sock_id.to_string(),
                duration.to_string(),
                elasticity2.to_string(),
            ])
            .unwrap();
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

    fn handle_timeout(&mut self) {
        self.handle_drop();
    }
}
