use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{sleep, Duration, Instant, Sleep};

pub struct Timer {
    duration: u64,
    sleep: Pin<Box<Sleep>>,
}

impl Timer {
    pub fn new(duration: u64) -> Self {
        let sleep = Box::pin(sleep(Duration::from_millis(duration)));
        Self { duration, sleep }
    }

    pub fn reset(&mut self) {
        self.sleep
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(self.duration));
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.sleep.as_mut().poll(cx)
    }
}
