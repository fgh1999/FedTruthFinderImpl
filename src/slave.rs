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

    // construct the slave_server
    // let cert = tokio::fs::read(client_side_config.keys.pk_path.clone()).await?;
    // let key = tokio::fs::read(client_side_config.keys.sk_path.clone()).await?;
    // let identity = Identity::from_pem(cert, key);
    let server_host_addr = client_side_config.addrs.mailbox_addr.clone();

    use slave_server::SlaveServerUtil;
    let logger_path = if args.len() >= 3 {
        Some(args[2].clone())
    } else {
        None
    };
    let server_for_algo_node_and_event_handler =
        slave_server::SlaveServer::build_slave_server(client_side_config, logger_path).await?;

    Server::builder()
        // .tls_config(ServerTlsConfig::new().identity(identity)).unwrap()
        .add_service(slave_server::slave_server::SlaveServer::new(
            server_for_algo_node_and_event_handler,
        ))
        .serve(server_host_addr.clone().parse().unwrap())
        .await?;
    Ok(())
}
