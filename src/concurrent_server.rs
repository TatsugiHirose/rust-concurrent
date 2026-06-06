mod test {
    use std::io::{BufRead, BufReader, BufWriter, Write};

    // 反復サーバー
    #[test]
    fn repitation_server_is_ok() {
        // `nc -v localhost:10000`でやまびこができる
        let listener = std::net::TcpListener::bind("127.0.0.1:10000").unwrap();

        while let Ok((stream, _)) = listener.accept() {
            let stream0 = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream0);
            let mut writer = BufWriter::new(stream);

            let mut buf = String::new();
            reader.read_line(&mut buf).unwrap();
            writer.write_all(buf.as_bytes()).unwrap();
            writer.flush().unwrap();
        }
    }
}
