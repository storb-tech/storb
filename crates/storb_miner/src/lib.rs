use tokio;

/// Run the miner
pub fn run() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            println!("Hello from the Storb miner");
        })
}
