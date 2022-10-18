use crate::ProxyState;
use std::sync::Arc;
use std::thread;
use std::time;

// the sliding window at most spans two time buckets and hence we only
// need to keep track of the latest two time buckets.
pub struct WindowTracker {
    last_bucket_counter: u32, // #requests forwarded to the upstream servers in the last time bucket.
    pub curr_bucket_counter: u32, // #requests forwarded to the upstream servers in the current time bucket.
    curr_bucket_start_time: time::Instant,
    window_size: time::Duration,
}

impl WindowTracker {
    pub fn new(window_size: u64) -> WindowTracker {
        WindowTracker {
            last_bucket_counter: 0,
            curr_bucket_counter: 0,
            curr_bucket_start_time: time::Instant::now(),
            window_size: time::Duration::from_secs(window_size),
        }
    }

    // increment the curr_bucket_counter by one.
    pub fn incre_counter(&mut self) {
        self.curr_bucket_counter += 1;
    }

    // slide the window to now, i.e. the right boundary of the window is set to now.
    // if this slide makes the window exceeds the boundary of the current time bucket,
    // the last time bucket is replaced with the current time bucket and the current time
    // bucket is replaced with the next time bucket, i.e. a new time bucket.
    pub fn slide_to(&mut self, now: time::Instant) {
        if now.duration_since(self.curr_bucket_start_time) > self.window_size {
            // slide window: replace the last bucket with the current one, and reset the current bucket.
            self.last_bucket_counter = self.curr_bucket_counter;
            self.curr_bucket_counter = 0;
            self.curr_bucket_start_time = now;

            log::debug!("slide window by one time bucket");
        }
    }

    // compute the request rate corresponding to the current window state.
    // the window state is specified with the two time buckets the window
    // spanning now and their time share on the window.
    pub fn get_request_rate(&mut self) -> u32 {
        // slide the window first.
        let now = time::Instant::now();
        self.slide_to(now);

        // weights the current time bucket and the last time buckets according to the
        // span of the sliding window.
        let curr_bucket_weight = now
            .duration_since(self.curr_bucket_start_time)
            .as_secs_f32()
            / self.window_size.as_secs_f32();
        let last_bucket_weight = 1.0 - curr_bucket_weight;
        log::debug!(
            "curr_bucket_weight = {},  last_bucket_weight = {}",
            curr_bucket_weight,
            last_bucket_weight
        );

        // compute the request rate, i.e. #requests in the sliding window of length window_size.
        // note, since this time bucket is not over, this means we've already taken into account
        // the sliding window span of the current time bucket. So there's no need to multiply
        // the curr_bucket_weight with curr_bucket_counter.num_requests.
        let request_rate = (last_bucket_weight * self.last_bucket_counter as f32).floor() as u32
            + self.curr_bucket_counter;

        request_rate
    }

    // upon forwarding a new client request to the upstream servers, the balancebeam
    // calls this function to do rate limiting check.
    // the request is only forwarded if passed the rate limiting check.
    //
    // return true if adding a new request would not exceed the rate limit.
    // return false otherwise.
    pub fn rate_limit_check(&mut self, state: &ProxyState) -> bool {
        let request_rate = self.get_request_rate();

        log::debug!(
            "current request_rate = {}, max_request_rate = {}",
            request_rate,
            state.max_request_rate
        );

        // pass the rate limit check if the current request rate plus the to-be-accepted request is not greater than the max request rate.
        request_rate + 1 <= state.max_request_rate as u32
    }
}

// slide the window every tick_interval secs.
// the balancebeam will spawn a dedicated thread to run this ticker.
pub fn sliding_window_ticker(state: Arc<ProxyState>, tick_interval: usize) {
    log::debug!("start sliding window ticker");

    let tick_interval = time::Duration::from_secs(tick_interval as u64);
    loop {
        let now = time::Instant::now();
        state.window_tracker.lock().unwrap().slide_to(now);

        while let Some(remaining_time) = tick_interval.checked_sub(now.elapsed()) {
            thread::sleep(remaining_time);
        }
    }
}
