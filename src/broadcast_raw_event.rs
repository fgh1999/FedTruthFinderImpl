tonic::include_proto!("algo");
use fed_truth_finder::event;
use slog::{error, info};

/// args: [program_name, event_num_limitation, listeners_url_path, event_data_path]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // init logger
    use slog::Drain;
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog::o!());

    if args.len() < 4 {
        error!(logger, "insufficient args"; "required" => 3, "given" => args.len());
        info!(logger, "args' meaning";
            "event_num_limitation" => "a positive integer; the allowed maximum number of events' raw records",
            "listeners_url_path" => "the path of urls file of event listeners",
            "event_data_path" => "the path of raw event data file",
        );
        return Err(anyhow::anyhow!("insufficient args"));
    }

    let event_num_limitation: usize = args[1].parse()?;
    let listeners_url_path = &args[2];
    let event_data_path = &args[3];

    let event_records =
        event::read_raw_event_records(event_data_path, event_num_limitation).await?;
    info!(logger, #"event", "loaded raw event records"; "number of raw event records" => event_records.len());
    let listener_urls = get_listeners_from(listeners_url_path)?;
    info!(logger, #"listener", "loaded listeners successfully"; "number of listeners" => listener_urls.len());

    let mut handles = Vec::new();
    for record in event_records.into_iter() {
        let duration = record.delay_seconds;
        let duration = tokio::time::Duration::from_secs_f64(duration);
        info!(logger, #"event publishment", "publishing a new event";
            "event_identifier" => record.identifier.clone(),
        );

        for listener_url in &listener_urls {
            let identifier = record.identifier.clone();
            let listener_url = listener_url.clone();
            let logger_clone = logger.clone();
            let handle = tokio::spawn(async move {
                let client = algo_node_client::AlgoNodeClient::connect(listener_url.clone()).await;
                match client {
                    Ok(mut client) => {
                        match client
                            .handle(EventNotification {
                                identifier: identifier.clone(),
                            })
                            .await
                        {
                            Ok(uid) => {
                                let uid = uid.into_inner();
                                info!(logger_clone, #"event publishment", "event handled successfully";
                                    "identifier" => identifier.clone(),
                                    "uid" => uid,
                                );
                            }
                            Err(e) => {
                                error!(logger_clone, #"event publishment", "listener failed to handle this event";
                                    "handler_url" => listener_url,
                                    "event_identifier" => identifier,
                                    "err" => %e,
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!(logger_clone, #"event publishment", "connection failed";
                            "handler_url" => listener_url,
                            "event_identifier" => identifier,
                            "err" => %e,
                        );
                    }
                }
            });
            handles.push(handle);
        }
        tokio::time::delay_for(duration).await;
    }
    futures::future::join_all(handles).await;

    Ok(())
}

fn get_listeners_from<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<Vec<String>> {
    let input = std::fs::read_to_string(path)?;
    Ok(input
        .split_whitespace()
        .filter(|x| !x.is_empty())
        .map(|x| x.to_string().trim().to_string())
        .collect())
}
