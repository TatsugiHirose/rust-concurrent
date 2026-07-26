// 5.3.2 IO多重化とasync/await

use std::collections::{HashMap, VecDeque};
use std::io::{BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{BorrowedFd, RawFd};
use std::sync::{Arc, Mutex};
use std::task::Waker;
use std::thread;

use nix::errno::Errno;
use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
use nix::unistd::write;

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

// kqueueだとeventfdが不要なので、以下の教科書の方法はとれなくなった。
// writeシステムコールではなくkqueueのイベント送信でやるので、IOSelectorのメソッドに持っていく
#[cfg(false)]
// eventfdに1を書き込むとIOSelectorに通知される。
// IOSelectorは読み込み後にeventfdに0を書き込む
fn write_eventfd(fd: RawFd, n: usize) {
    // nの値をメモリ上のバイト列に変換しているらしい
    let ptr = &n as *const usize as *const u8;
    let val = unsafe { std::slice::from_raw_parts(ptr, std::mem::size_of_val(&n)) };

    // なんか教科書のやり方だとエラーが出たのでRawFdをnixが受けれる型に変換する
    write(unsafe { BorrowedFd::borrow_raw(fd) }, val).unwrap();
}

enum IOOps {
    Add(EvFlags, RawFd, Waker),
    Remove(RawFd),
}

struct IOSelector {
    wakers: Mutex<HashMap<RawFd, Waker>>,
    queue: Mutex<VecDeque<IOOps>>,
    // macだからkqueue。ちなみに教科書はRawFdにしてるがepollも今ではそれを非推奨。
    epfd: Kqueue,
    // KEventはSyncを持たないので、kEventのident()が一応相当しそうな気がする。
    event: usize, // eventfd(Linuxのイベント通知I/F)のfd。
}

impl IOSelector {
    fn new() -> Arc<Self> {
        // `event: eventfd(0, EfdFlags::empty()).unwrap(),`に当たりそうな `KEvent::new()`でやるつもりだったが、KEventはSyncを持たないので、フィールドに持たせることができない。なので、ident()を使うことにする。
        // また、kqueueでは、KEvent作成だけでは、監視対象に追加されないので、epfd.kevent()で追加する必要がある。
        // なので、IOSelectorインスタンス作成前にゴニョゴニョする必要がある。

        // epoll_create1(EpollCreateFlags::empty()) の代わり
        let epfd = Kqueue::new().unwrap();

        // eventfd(0, EfdFlags::empty()) の代わり
        let event = KEvent::new(
            0,                                   // 0で良いのかなぁ？
            EventFilter::EVFILT_USER,            // eventfdの代わりになるらしい
            EvFlags::EV_ADD | EvFlags::EV_CLEAR, // が良いらしい。
            FilterFlag::empty(),
            0,
            0,
        );
        epfd.kevent(
            &[event],
            &mut [], // 待機なし。登録のみ。
            None,
        )
        .unwrap();

        let s = Self {
            wakers: Mutex::new(HashMap::new()),
            queue: Mutex::new(VecDeque::new()),
            // epoll_create1(EpollCreateFlags::empty()) の代わり
            epfd,
            event: event.ident(),
        };

        let result = Arc::new(s);
        let s = result.clone();
        thread::spawn(move || s.select()); // kqueue用スレッド
        result
    }

    fn select(&self) {
        //　以下の代わりをやろうとしていたが、kqueueだと不要説がある。（kqueueでは、ユーザイベントは登録時に即座に監視対象になるため）
        // ```rs
        // let mut ev = EpollEvent::new(EpollFlags::EPOLLIN, self.event as u64);
        // epollctrl(self.epfd, EpollOp::EpollCtlAdd, self.event, &mut ev).unwrap();
        // ```
        // let ev = KEvent::new(
        //     self.event, // eventfdを監視ってことか
        //     EventFilter::EVFILT_READ,
        //     EvFlags::EV_ADD,
        //     FilterFlag::empty(),
        //     0,
        //     self.event as isize, // イベントが帰る時はこの値を返すらしい
        // );
        // self.epfd.kevent(&[ev], &mut [], None).unwrap();

        // epoll_waitと同じことをする
        let mut events = vec![
            KEvent::new(
                0,
                EventFilter::EVFILT_READ,
                EvFlags::empty(),
                FilterFlag::empty(),
                0,
                0,
            );
            1024
        ];
        while let Ok(nfds) = self.epfd.kevent(&[], &mut events, None) {
            let mut wakers = self.wakers.lock().unwrap();
            for event in events.iter().take(nfds) {
                if event.ident() == self.event {
                    // eventfdのイベントの場合
                    let mut q = self.queue.lock().unwrap();
                    while let Some(op) = q.pop_front() {
                        match op {
                            IOOps::Add(flag, fd, waker) => {
                                self.add_event(flag, fd, waker, &mut wakers)
                            }
                            IOOps::Remove(fd) => self.rm_event(fd, &mut wakers),
                        }
                    }
                } else {
                    // eventfd以外のイベント
                    // 実行キューに追加
                    let fd = event.udata() as RawFd;
                    let waker = wakers.remove(&fd).unwrap();
                    waker.wake_by_ref();
                }
            }
        }
    }

    // kqueueで監視するための関数。
    // ファイルディスクリプタのkqueueへの登録と、Wakerの紐付け
    fn add_event(
        &self,
        flag: EvFlags,
        fd: RawFd,
        waker: Waker,
        wakers: &mut HashMap<RawFd, Waker>,
    ) {
        let event = KEvent::new(
            fd as usize,
            EventFilter::EVFILT_READ,
            flag | EvFlags::EV_ADD | EvFlags::EV_ONESHOT,
            FilterFlag::empty(),
            0,
            fd as isize,
        );
        if let Err(err) = self.epfd.kevent(&[event], &mut [], None) {
            match err {
                Errno::EEXIST => {
                    //　すでに登録されていたら再設定（kqueueはADDしかないしADDが上書きするらしいので、必要なのかは不明だが）
                    self.epfd.kevent(&[event], &mut [], None).unwrap();
                }
                _ => panic!("epoll_ctl {err}"),
            }
        }
        assert!(wakers.contains_key(&fd));
        wakers.insert(fd, waker);
    }
    fn rm_event(&self, fd: RawFd, wakers: &mut HashMap<RawFd, Waker>) {
        let event = KEvent::new(
            fd as usize,
            EventFilter::EVFILT_READ,
            EvFlags::EV_DELETE,
            FilterFlag::empty(),
            0,
            fd as isize,
        );
        self.epfd.kevent(&[event], &mut [], None).unwrap();
        wakers.remove(&fd);
    }

    // ファイルディスクリプタの監視登録と解除を行う関数
    fn register(&self, flags: EvFlags, fd: RawFd, waker: Waker) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(IOOps::Add(flags, fd, waker));
        self.write_eventfd(-1, 1);
    }
    fn unregister(&self, fd: RawFd) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(IOOps::Remove(fd));
        self.write_eventfd(-1, 1);
    }

    // kqueueだとeventfdが不要なので、教科書の方法が取れなかったので、IOSelectorのメソッドとして作ることにした
    fn write_eventfd(&self, _fd: RawFd, _n: usize) {
        let event = KEvent::new(
            self.event,
            EventFilter::EVFILT_USER,
            EvFlags::EV_ADD,
            FilterFlag::NOTE_TRIGGER,
            0,
            0,
        );
        self.epfd.kevent(&[event], &mut [], None).unwrap();
    }
}

struct AsyncListener {
    listener: TcpListener,
    selector: Arc<IOSelector>,
}

impl AsyncListener {
    fn listen(host: &str, selector: Arc<IOSelector>) -> Self {
        let listener = TcpListener::bind(host).unwrap();

        // ノンブロッキングを指定
        listener.set_nonblocking(true).unwrap();

        Self { listener, selector }
    }

    // 実際にAcceptを行わず、Acceptを行うFutureをリターンする
    fn accept(&self) -> Accept {
        self.listener.accept();
        Accept {}
    }
}

// コネクションをAcceptするためのFuture
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
