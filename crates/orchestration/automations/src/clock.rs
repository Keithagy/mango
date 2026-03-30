use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

pub trait Clock: Clone + Send + Sync + 'static {
    fn now(&self) -> i64;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};

        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
            })
    }
}

#[derive(Debug, Clone)]
pub struct ManualClock {
    now: Arc<AtomicI64>,
}

impl ManualClock {
    #[must_use]
    pub fn new(initial_timestamp: i64) -> Self {
        Self {
            now: Arc::new(AtomicI64::new(initial_timestamp)),
        }
    }

    pub fn set(&self, timestamp: i64) {
        self.now.store(timestamp, Ordering::SeqCst);
    }

    pub fn advance_by(&self, seconds: i64) {
        self.now.fetch_add(seconds, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}
