use std::time::{Duration, Instant};
use tracing::debug;

pub struct Cubic {
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
    mss: u32,
    last_drop: Instant,
}

impl Cubic {
    pub fn new(mss: u32) -> Self {
        Self {
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

            mss,
            last_drop: Instant::now(),
        }
    }

    pub fn curr_cwnd_bytes(&self) -> f64 {
        self.cubic_cwnd * 1448.
    }

    pub fn drop(&mut self, sock_id: u32, rtt: Duration) {
        let now = Instant::now();
        if (now - self.last_drop) < rtt {
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

        debug!(
            ID = sock_id,
            time_since_last_drop = (now - self.last_drop).as_secs_f64(),
            rtt = ?rtt,
            "[nimbus cubic] got drop"
        );
        self.last_drop = now;
    }

    pub fn update_rate(&mut self, new_bytes_acked: u64, rtt: Duration) {
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

        let rtt_seconds = rtt.as_secs_f64();
        for _ in 0..no_of_acks as usize {
            if self.d_min <= 0.0 || rtt_seconds < self.d_min {
                self.d_min = rtt_seconds;
            }

            self.update();
            if self.cwnd_cnt > self.cnt {
                self.cubic_cwnd += 1.0;
                self.cwnd_cnt = 0.0;
            } else {
                self.cwnd_cnt += 1.0;
            }
        }
    }

    fn update(&mut self) {
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
            self.tcp_friendliness();
        }
    }

    fn tcp_friendliness(&mut self) {
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
}
