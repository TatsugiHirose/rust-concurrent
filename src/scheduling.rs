// 5.2.2 スケジューリング

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, SyncSender, sync_channel},
    },
    task::{Context, Poll},
};

use futures::{
    FutureExt,
    future::BoxFuture,
    task::{ArcWake, waker_ref},
};

struct Task {
    future: Mutex<BoxFuture<'static, ()>>,
    sender: SyncSender<Arc<Self>>, // (Senderは複製前提なのでArc不要らしい)
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let self0 = arc_self.clone();
        arc_self.sender.send(self0).unwrap();
    }
}

struct Executor {
    // 実行キュー
    sender: SyncSender<Arc<Task>>,
    receiver: Receiver<Arc<Task>>,
}

impl Executor {
    fn new() -> Self {
        let (sender, receiver) = sync_channel(1024);
        Self { sender, receiver }
    }

    fn get_spawner(&self) -> Spawner {
        Spawner {
            sender: self.sender.clone(),
        }
    }

    fn run(&self) {
        while let Ok(task) = self.receiver.recv() {
            let waker = waker_ref(&task);
            let mut ctx = Context::from_waker(&waker); // ここで3rdpartyのfuture-taskの世界から、標準のContextに変換

            let mut future = task.future.lock().unwrap();

            let _ = future.as_mut().poll(&mut ctx); // Hello実装してない時点でもエラーなく書けた。おもろ。
        }
    }
}

struct Spawner {
    sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let task = Arc::new(Task {
            future: Mutex::new(future.boxed()),
            sender: self.sender.clone(),
        });
        self.sender.send(task).unwrap();
    }
}

struct Hello {
    state: HelloState,
}

enum HelloState {
    HELLO,
    WORLD,
    END,
}

impl Hello {
    fn new() -> Self {
        Self {
            state: HelloState::HELLO,
        }
    }
}

impl Future for Hello {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &self.state {
            HelloState::HELLO => {
                print!("Hello ");
                self.state = HelloState::WORLD;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            HelloState::WORLD => {
                println!("World!");
                self.state = HelloState::END;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            HelloState::END => Poll::Ready(()),
        }
    }
}

#[test]
fn main_scheculing() {
    let executor = Executor::new();
    executor.get_spawner().spawn(Hello::new());
    executor.run();
}
