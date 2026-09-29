//! Small xorshift RNG for random poll start. Keeps polling fair.

use std::cell::Cell;

fn seed() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    use std::sync::atomic::{AtomicU32, Ordering::Relaxed};

    static COUNTER: AtomicU32 = AtomicU32::new(1);
    RandomState::new().hash_one(COUNTER.fetch_add(1, Relaxed))
}

#[derive(Debug)]
struct FastRand {
    one: Cell<u32>,
    two: Cell<u32>,
}

impl FastRand {
    fn new(seed: u64) -> Self {
        let one = (seed >> 32) as u32;
        let mut two = seed as u32;
        if two == 0 {
            two = 1;
        }
        Self {
            one: Cell::new(one),
            two: Cell::new(two),
        }
    }

    fn fastrand_n(&self, n: u32) -> u32 {
        // Lemire's fast mod reduction, avoids `%`.
        let mul = (self.fastrand() as u64).wrapping_mul(n as u64);
        (mul >> 32) as u32
    }

    fn fastrand(&self) -> u32 {
        let mut s1 = self.one.get();
        let s0 = self.two.get();
        s1 ^= s1 << 17;
        s1 = s1 ^ s0 ^ s1 >> 7 ^ s0 >> 16;
        self.one.set(s0);
        self.two.set(s1);
        s0.wrapping_add(s1)
    }
}

/// Random number in `0..n`. Caller must ensure `n > 0`.
pub(crate) fn thread_rng_n(n: u32) -> u32 {
    thread_local! {
        static THREAD_RNG: FastRand = FastRand::new(seed());
    }
    THREAD_RNG.with(|rng| rng.fastrand_n(n))
}
