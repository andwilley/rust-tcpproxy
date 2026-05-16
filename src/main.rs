// use std::env;
use tcpproxy::proxy;

#[tokio::main]
async fn main() {
    // needed for config eventually
    // let arg: Vec<String> = env().collect();
    proxy::proxy_server().await.ok();
}
