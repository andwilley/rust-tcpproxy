use anyhow::Result;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

pub async fn proxy_server() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:3000").await?;
    loop {
        println!("awaiting connection");
        let (mut in_sock, addr) = listener.accept().await?;
        println!("new client {:?} on 127.0.0.1:3000", addr);

        // These need timeouts (or keepalives)
        tokio::spawn(async move {
            let mut out_sock = TcpStream::connect("127.0.0.1:4000").await.unwrap();
            println!("connecting 127.0.0.1:3000 to 127.0.0.1:4000");
            tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock)
                .await
                .unwrap();
            println!("closed connection from {:?}", addr);
        });
    }
}
