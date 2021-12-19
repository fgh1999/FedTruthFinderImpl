use fed_truth_finder::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("insufficient args (args: config_path[, log_path])");
        std::process::exit(-1);
    }

    // init slog
    use slog::Drain;
    let logger = if args.len() >= 3 {
        // log to file
        let log_path = &args[2];
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(log_path)
            .unwrap(); // will overwrite existing content

        let decorator = slog_term::PlainDecorator::new(file);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    } else {
        // log to terminal
        let decorator = slog_term::TermDecorator::new().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, slog::o!())
    };
    slog::info!(logger, #"master", "logger initiated");

    slog::info!(logger, #"master", "launching server...");
    let server_config_path = &args[1];
    run_tls_server(server_config_path, logger.clone()).await;
    Ok(())
}

async fn run_tls_server(config_path: impl AsRef<std::path::Path>, logger: slog::Logger) {
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    let config = config::load_server_config(config_path.as_ref().to_str().unwrap());
    // let cert = tokio::fs::read(config.keys.pk_path).await.unwrap();
    // let key = tokio::fs::read(config.keys.sk_path).await.unwrap();
    // let identity = Identity::from_pem(cert, key);
    let addr = config.addr.parse().unwrap();
    let master = master_server::MasterServer::new(
        config.group_num as id::Gid,
        config.client_num,
        config.error_rate,
        logger.clone(),
    );
    let master = Arc::new(master);
    let master_ = master.clone();
    tokio::spawn(iterate(master_, logger));
    Server::builder()
        // .tls_config(ServerTlsConfig::new().identity(identity)).unwrap()
        .add_service(master_server::registration_server::RegistrationServer::new(
            master.clone(),
        ))
        .add_service(master_server::security_server::SecurityServer::new(
            master,
        ))
        .serve(addr)
        .await
        .unwrap();
}

async fn iterate(master: Arc<master_server::MasterServer>, logger: slog::Logger) -> anyhow::Result<()> {
    use master_server::{Iteration, Rank};
    master.wrapped_iterate().await;

    match master.rank_clients_trustworthiness().await {
        Ok(ranking_list) => {
            let mut leader_board = String::new();
            ranking_list.into_iter().for_each(|uid| {
                let uid = format!("{} ", uid);
                leader_board.push_str(&uid);
            });
            slog::info!(logger, "Trustworthiness leader-board"; "ascending" => leader_board);
        },
        Err(e) => {
            slog::error!(logger, "Trustworthiness ranking ends with an error"; "error" => %e);
        }
    }

    Ok(())
}
