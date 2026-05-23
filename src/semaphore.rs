use std::sync::{Condvar, Mutex};

pub struct Semaphore {
    mutex: Mutex<isize>,
    condvar: Condvar,
    max: isize,
}

impl Semaphore {
    pub fn new(max: isize) -> Self {
        Semaphore {
            mutex: Mutex::new(0),
            condvar: Condvar::new(),
            max,
        }
    }

    pub fn wait(&self) {
        let mut cnt = self.mutex.lock().unwrap();
        while *cnt >= self.max {
            // なんでwhileなんだろう
            cnt = self.condvar.wait(cnt).unwrap();
        }
        *cnt += 1;
    }

    pub fn post(&self) {
        let mut cnt = self.mutex.lock().unwrap();
        *cnt -= 1;
        if *cnt <= self.max {
            // なんでこのifが要るんだろう
            self.condvar.notify_one();
        }
    }
}

mod test {
    #[test]
    fn semaphore_is_ok() {
        use crate::semaphore::Semaphore;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const SEM_NUM: isize = 4;
        const NUM_THREADS: usize = 8;
        const NUM_LOOP: usize = 100000;

        static CNT: AtomicUsize = AtomicUsize::new(0);

        let mut v = Vec::new();
        let sem = Arc::new(Semaphore::new(SEM_NUM));

        for i in 0..NUM_THREADS {
            let s = sem.clone();
            let t = std::thread::spawn(move || {
                for _ in 0..NUM_LOOP {
                    s.wait();

                    // セマフォが効いてるかをチェックするために、アトミックなカウントアップした値を確認
                    CNT.fetch_add(1, Ordering::SeqCst);
                    let n = CNT.load(Ordering::SeqCst);
                    println!("semaphore: i = {i}, CNT = {n}"); // CNTが4以下なら良い
                    assert!(n as isize <= SEM_NUM);
                    CNT.fetch_sub(1, Ordering::SeqCst);

                    s.post();
                }
            });

            v.push(t);
        }

        for t in v {
            t.join().unwrap()
        }
    }
}
