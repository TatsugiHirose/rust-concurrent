// 5.3.2 IO多重化とasync/await

use std::io::{BufWriter, Write};
use std::net::TcpStream;

struct Executor {}

impl Executor {
    fn new() -> Self {
        Self {}
    }

    fn get_spawner(&self) -> Spawner {
        Spawner {}
    }

    fn run(&self) {}
}

struct Spawner {}

impl Spawner {
    fn spawn(&self, future: impl Future) {}
}

#[derive(Clone)]
struct IOSelector {}

impl IOSelector {
    fn new() -> Self {
        Self {}
    }
}

struct AsyncListener {}

impl AsyncListener {
    fn listen(host: &str, selector: IOSelector) -> Self {
        Self {}
    }

    fn accept(&self) -> Accept {
        Accept {}
    }
}

struct Accept {}

impl Future for Accept {
    type Output = (AsyncReader, BufWriter<TcpStream>, &'static str);

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unimplemented!()
    }
}

struct AsyncReader {}

impl AsyncReader {
    fn read_line(&self) -> ReadLine {
        ReadLine {}
    }
}

struct ReadLine {}

impl Future for ReadLine {
    type Output = Option<String>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unimplemented!()
    }
}

// 先にmainから書いておくか。
#[test]
fn main() {
    let executor = Executor::new();
    let selector = IOSelector::new();
    let spawner = executor.get_spawner();

    let server = async move {
        let listener = AsyncListener::listen("127.0.0.1:10000", selector.clone());

        loop {
            let (mut reader, mut writer, addr) = listener.accept().await;

            spawner.spawn(async move {
                while let Some(buf) = reader.read_line().await {
                    println!("read: {addr}, {buf}");
                    writer.write_all(buf.as_bytes()).unwrap();
                    writer.flush().unwrap();
                }
            });
            println!("close: {addr}");
        }
    };

    executor.get_spawner().spawn(server);
    executor.run();
}
