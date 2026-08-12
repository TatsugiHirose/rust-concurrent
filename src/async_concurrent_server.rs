// 5.3.2 IO多重化とasync/await

use std::collections::{HashMap, VecDeque};
use std::io::{BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::task::{ArcWake, waker_ref};
use nix::errno::Errno;
use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};

/// 簡易のため、Future自身がWakerを持つようにしているらしい。
struct Task {
    /// 実行するコルーチン
    ///
    /// - BoxFutureはfuturesクレートの型。ほぼPin（メモリ移動しない型）に相当。
    /// - poll()は &mut selfで呼ぶので、内部可変性を持たせるためにMutexにしているっぽい。
    ///   これにより &Task からでも poll() が呼べるようになる。
    /// - または、Executorが複数スレッドで動かす場合に、Mutexによって poll() の同時実行を防ぐことができる。
    future: Mutex<BoxFuture<'static, ()>>,

    /// Executorへスケジューリングするためのチャネル
    sender: SyncSender<Arc<Self>>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let self0 = arc_self.clone();
        arc_self.sender.send(self0).unwrap();
    }
}

struct Executor {
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
            // pollを実行するためにはcontextが必要だから、作る
            let waker = waker_ref(&task);
            let mut ctx = Context::from_waker(&waker); // ここでやっとstdの型に合流

            // poll
            let mut future = task.future.lock().unwrap();
            let _ = future.as_mut().poll(&mut ctx);
        }
    }
}

struct Spawner {
    sender: SyncSender<Arc<Task>>,
}

impl Spawner {
    fn spawn(&self, future: impl Future<Output = ()> + 'static + Send) {
        let future = future.boxed();
        let task = Task {
            future: Mutex::new(future),
            sender: self.sender.clone(),
        };
        self.sender.send(Arc::new(task)).unwrap();
    }
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
    // イベントの追加及びWakerの追加を行うっぽい
    Add(EvFlags, RawFd, Waker),
    // イベントの削除及びWakerの削除を行うっぽい
    Remove(RawFd),
}

struct IOSelector {
    wakers: Mutex<HashMap<RawFd, Waker>>,
    queue: Mutex<VecDeque<IOOps>>,
    // epollのfd。macだからkqueue。ちなみに教科書はRawFdにしてるがepollも今ではそれを非推奨。
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
                    // eventfd以外のイベント(どういうイベントが来るのだろうか？newの時点では、eventfdしか登録していないはずだが)
                    //    ↑ 上のAddとかRemoveで登録してたわ。
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

/// 非同期にTCPのリッスンとAcceptを行うための構造体
///
/// 重要なのは、Acceptを行うときに、Accept用の関数を直接呼ぶのではなく、Acceptを行うFutureを返すこと。
struct AsyncListener {
    listener: TcpListener,
    selector: Arc<IOSelector>,
}

impl AsyncListener {
    fn listen(host: &str, selector: Arc<IOSelector>) -> Self {
        let listener = TcpListener::bind(host).unwrap();

        // TCPリスナーにノンブロッキングを指定
        listener.set_nonblocking(true).unwrap(); // 接続待ちになるなら、スレッドを止めず、即座にエラーを返す挙動になるだけらしい。

        Self { listener, selector }
    }

    // コネクションをAcceptするためのFutureをリターンする
    fn accept(&self) -> Accept {
        Accept { listener: self }
    }
}
impl Drop for AsyncListener {
    fn drop(&mut self) {
        // epollへの登録を解除する
        self.selector.unregister(self.listener.as_raw_fd());
    }
}

// コネクションをAcceptするためのFuture
struct Accept<'a> {
    listener: &'a AsyncListener,
}

impl<'a> Future for Accept<'a> {
    type Output = (AsyncReader, BufWriter<TcpStream>, SocketAddr);

    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // ノンブロッキングモードにしたので、accept()はすぐに返る。接続がなければエラーになるはず。
        match self.listener.listener.accept() {
            Ok((stream, addr)) => {
                let stream0 = stream.try_clone().unwrap();
                Poll::Ready((unimplemented!(), BufWriter::new(stream), addr))
            }
            Err(_) => {
                // TODO: もっとやらないといけないことあるよ
                Poll::Pending
            }
        }
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
