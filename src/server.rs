use tonic::transport::Server;
use fed_truth_finder::echo;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (addr, tls_configuration) = echo::init_server("config/server.json").await?;

    Server::builder()
        .tls_config(tls_configuration)?
        .add_service(echo::new_echo_servive())
        .serve(addr)
        .await?;

    Ok(())
}
