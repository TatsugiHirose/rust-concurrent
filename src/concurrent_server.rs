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
        }
    }
}
