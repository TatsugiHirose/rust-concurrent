// 5.2.1 コルーチン

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::task::ArcWake;

struct Hello {
    state: StateHello,
}

enum StateHello {
    HELLO,
    WORLD,
    END,
}

impl Hello {
    fn new() -> Self {
        Self {
            state: StateHello::HELLO,
        }
    }
}

impl Future for Hello {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state {
            StateHello::HELLO => {
                print!("Hello, ");
                self.state = StateHello::WORLD;
                Poll::Pending
            }
            StateHello::WORLD => {
                println!("World!");
                self.state = StateHello::END;
                Poll::Pending
            }
            StateHello::END => Poll::Ready(()),
        }
    }
}

// 実行単位
struct Task {
    // これ外部クレートの型だったのか。がっかり。
    hello: Mutex<BoxFuture<'static, ()>>,
}

impl Task {
    fn new() -> Self {
        let hello = Hello::new();
        Self {
            hello: Mutex::new(hello.boxed()),
        }
    }
}

impl ArcWake for Task {
    fn wake_by_ref(_arc_self: &Arc<Self>) {}
}

#[cfg(test)]
mod tests {
    use futures::task::waker_ref;

    use super::*;

    #[test]
    fn helloコルーチンを使う() {
        let task = Arc::new(Task::new());
        let waker = waker_ref(&task);
        let mut ctx = Context::from_waker(&waker); // ここでRust標準の世界に戻る

        let mut hello = task.hello.lock().unwrap();

        let _ = hello.as_mut().poll(&mut ctx);
        let _ = hello.as_mut().poll(&mut ctx);
        let _ = hello.as_mut().poll(&mut ctx);
    }
}
