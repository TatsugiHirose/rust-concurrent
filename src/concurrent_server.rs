mod test {
    use std::io::{BufRead, BufReader, BufWriter, Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::os::fd::AsRawFd;
    use std::ptr::read;
    use std::thread;

    use nix::sys::event;

    // 反復サーバー
    #[test]
    fn repitation_server_is_ok() {
        // `nc -v localhost 10000`でやまびこができる
        let listener = TcpListener::bind("127.0.0.1:10000").unwrap();

        while let Ok((stream, _)) = listener.accept() {
            let stream0 = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream0);
            let mut writer = BufWriter::new(stream);

            let mut buf = String::new();
            reader.read_line(&mut buf).unwrap();
            println!("read: {buf}");
            writer.write_all(buf.as_bytes()).unwrap();
            writer.flush().unwrap();
            println!("write: {buf}");
        }
    }

    #[test]
    fn 反復サーバーに多重でアクセスしてみる() {
        //　スレッドなし 直列アクセス
        for i in 0..10 {
            let mut connection = TcpStream::connect("127.0.0.1:10000").unwrap();
            let buf = format!("number {i}\n");
            connection.write_all(buf.as_bytes()).unwrap();
            println!("write: {buf}");

            let mut reader = BufReader::new(connection);
            let mut buf = String::new();
            reader.read_line(&mut buf).unwrap();
            println!("read: {buf}");
        }

        // スレッドあり 多重アクセス
        let mut v = Vec::new();
        for i in 0..10 {
            let th = std::thread::spawn(move || {
                let mut connection = TcpStream::connect("127.0.0.1:10000").unwrap();
                let buf = format!("thread number {i}\n");
                connection.write_all(buf.as_bytes()).unwrap();
                println!("write: {buf}");

                let mut reader = BufReader::new(connection);
                let mut buf = String::new();
                reader.read_line(&mut buf).unwrap();
                println!("read: {buf}");
            });
            v.push(th);
        }
        for th in v {
            th.join().unwrap()
        }
    }

    #[test]
    fn kqueue並行サーバー() {
        use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};
        use std::collections::HashMap;

        let listener = TcpListener::bind("127.0.0.1:10000").unwrap();

        // リッスン用のソケットを監視対象に追加
        let listen_fd = listener.as_raw_fd();
        let ev = KEvent::new(
            listen_fd as usize, // 監視対象らしい
            EventFilter::EVFILT_READ,
            EvFlags::EV_ADD,
            FilterFlag::empty(),
            0,
            listen_fd as isize, // イベントが返る時はこの値を返すらしい
        );

        // `epoll_ctl(epfd, EpolOp::EpollCtlAdd, listen_fd, &mut ev);`　に相当。
        let epfd = Kqueue::new().unwrap();
        epfd.kevent(
            &[ev],
            &mut [], // 空にすることで、待機なしで、登録のみができる。逆に入れると待機してしまう。
            None,
        )
        .unwrap();

        let mut fd2buf = HashMap::new();
        let mut events = vec![
            KEvent::new(
                0,
                EventFilter::EVFILT_READ,
                EvFlags::empty(),
                FilterFlag::empty(),
                0,
                0
            );
            1024
        ];

        // kqueueでイベント発生を監視
        while let Ok(nfds) = epfd.kevent(&[], &mut events, None) {
            for i in 0..nfds {
                if events[i].ident() == listen_fd as usize {
                    // リッスンソケットにイベント（接続要求ということか）
                    if let Ok((stream, _)) = listener.accept() {
                        let fd = stream.as_raw_fd();
                        let stream0 = stream.try_clone().unwrap();
                        let reader = BufReader::new(stream0);
                        let writer = BufWriter::new(stream);

                        fd2buf.insert(fd, (reader, writer));

                        println!("accept fd = {}", fd);

                        //
                        let ev = KEvent::new(
                            fd as usize,
                            EventFilter::EVFILT_READ,
                            EvFlags::EV_ADD,
                            FilterFlag::empty(),
                            0,
                            fd as isize,
                        );
                        epfd.kevent(&[ev], &mut [], None).unwrap();
                    }
                } else {
                    // クライアントからデータ到着
                    let fd = events[i].udata() as i32;
                    let (reader, writer) = fd2buf.get_mut(&fd).unwrap();

                    let mut buf = String::new();
                    let n = reader.read_line(&mut buf).unwrap();

                    // コネクションクローズした場合、監視対象から外す
                    if n == 0 {
                        let ev = KEvent::new(
                            fd as usize,
                            EventFilter::EVFILT_READ,
                            EvFlags::EV_DELETE,
                            FilterFlag::empty(),
                            0,
                            0,
                        );
                        epfd.kevent(&[ev], &mut [], None).unwrap();

                        fd2buf.remove(&fd); // kqueueならこれやっておけば、↑のDELETEは要らないらしい。
                        println!("closed: fd = {fd}");
                        continue;
                    }

                    println!("read: fd = {fd}, buf = {buf}");

                    writer.write_all(buf.as_bytes()).unwrap();
                    writer.flush().unwrap();
                    println!("write: fd = {fd}, buf = {buf}");
                }
            }
        }
    }
}
