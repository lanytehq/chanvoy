#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    chanvoy_cli::run().await?;
    Ok(())
}
