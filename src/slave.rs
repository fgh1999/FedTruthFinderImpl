use fed_truth_finder::*;
use tonic::transport::{Identity, Server, ServerTlsConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("insufficient args (args: config_path[, log_path])");
    }
    let config_path = &args[1];
    let client_side_config = config::load_client_config(config_path.as_str());

    // construct the algo_node_server
    // let cert = tokio::fs::read(client_side_config.keys.pk_path.clone()).await?;
    // let key = tokio::fs::read(client_side_config.keys.sk_path.clone()).await?;
    // let identity = Identity::from_pem(cert, key);
    let server_host_addr = client_side_config.addrs.mailbox_addr.clone();

    use algo_client::AlgoClientUtil;
    let logger_path = if args.len() >= 3 {
        Some(args[2].clone())
    } else {
        None
    };
    let server_for_algo_node_and_event_handler =
        algo_client::AlgoClient::new_algo_client(client_side_config, logger_path).await?;

    Server::builder()
        // .tls_config(ServerTlsConfig::new().identity(identity)).unwrap()
        .add_service(algo_client::algo_node_server::AlgoNodeServer::new(
            server_for_algo_node_and_event_handler,
        ))
        .serve(server_host_addr.clone().parse().unwrap())
        .await?;
    Ok(())
}
