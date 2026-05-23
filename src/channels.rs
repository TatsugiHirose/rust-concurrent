use crate::semaphore::Semaphore;
use std::{
    collections::LinkedList,
    sync::{Arc, Condvar, Mutex},
};

#[derive(Clone)]
pub struct Sender<T> {
    sem: Arc<Semaphore>,
    buf: Arc<Mutex<LinkedList<T>>>,
    cond: Arc<Condvar>,
}

impl<T: Send> Sender<T> {
    pub fn send(&self, data: T) {
        self.sem.wait();

        let mut buf = self.buf.lock().unwrap();
        buf.push_back(data);

        self.cond.notify_one(); // receiverにキューしたことを知らせる

        // データ受け取り完了を以て、次のスレッドが入れるようになるということだから、セマフォ片付けはreceiver側の仕事
        // だからsenderはここで仕事を終えちゃって良い。
    }
}

pub struct Receiver<T> {
    sem: Arc<Semaphore>,
    buf: Arc<Mutex<LinkedList<T>>>,
    cond: Arc<Condvar>,
}

impl<T> Receiver<T> {
    pub fn recv(&self) -> T {
        // self.sem.wait();  // receiverは一つしかないのでセマフォを待つ必要はない。

        let mut buf = self.buf.lock().unwrap();
        loop {
            if let Some(data) = buf.pop_front() {
                self.sem.post(); // 受け取ったら、次の送信者を許可する
                return data;
            }

            // キューが空だったら待機し、状態変数で起こしてもらう
            buf = self.cond.wait(buf).unwrap(); // waitによる新しいロックは、返り値なのでちゃんと変数で受け取ること
        }
    }
}

pub fn channels<T>(max: isize) -> (Sender<T>, Receiver<T>) {
    let sem = Arc::new(Semaphore::new(max));
    let buf = Arc::new(Mutex::new(LinkedList::new()));
    let cond = Arc::new(Condvar::new());

    let sender = Sender {
        sem: sem.clone(),
        buf: buf.clone(),
        cond: cond.clone(),
    };
    let receiver = Receiver { sem, buf, cond };
    (sender, receiver)
}

mod test {
    #[test]
    fn channels_is_ok() {
        use super::channels;

        const NUM_THREADS: usize = 8;
        const NUM_LOOP: usize = 100000;

        let mut v = Vec::new();
        let (tx, rx) = channels(4);

        let t = std::thread::spawn(move || {
            let mut cnt = 0;
            while cnt < NUM_THREADS * NUM_LOOP {
                let data = rx.recv();
                println!("recv n = {:?}", data);
                cnt += 1;
            }
        });
        v.push(t);

        for i in 0..NUM_THREADS {
            let tx0 = tx.clone();
            let t = std::thread::spawn(move || {
                for j in 0..NUM_LOOP {
                    tx0.send((i, j));
                }
            });
            v.push(t);
        }

        for t in v {
            t.join().unwrap();
        }
    }
}
