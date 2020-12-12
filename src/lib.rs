pub mod algo_client;
pub mod algo_server;
pub mod config;
pub mod event;
pub mod id;

pub mod deamon;
pub use deamon::*;

use futures::Stream;
use std::pin::Pin;
use tonic::{Response, Status};
pub type ResponseResult<T> = Result<Response<T>, Status>;
pub type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

pub fn fmt_leader_board(leader_board: Vec<id::Uid>) -> String {
    use std::fmt::Write as FmtWrite;
    let mut content = String::new();
    for rank in leader_board {
        write!(&mut content, " {}", rank).unwrap();
    }
    content
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio::time::{delay_for, Duration};

    #[tokio::test]
    async fn nodes() -> anyhow::Result<()> {
        // hard_code test configurations
        let client_n = 6;
        let event_num_limitation = 20;

        let mut thread_handles = Vec::with_capacity(client_n + 1);
        let (tx, _) = tokio::sync::broadcast::channel(event_num_limitation);
        for i in 1..=client_n {
            let event_src = tx.subscribe();
            let config_file_path = format!("config/client_{}.json", i);
            let a_new_client = tokio::spawn(async move {
                run_tls_client(config_file_path, event_src).await;
            });
            thread_handles.push(a_new_client);
            delay_for(Duration::from_secs(1)).await;
        }

        let results = futures::future::join_all(thread_handles).await;
        let errs: Vec<_> = results
            .iter()
            .filter(|result| result.is_err())
            .map(|result| result.as_ref().unwrap_err())
            .collect();
        if errs.is_empty() {
            Ok(())
        } else {
            for err in errs {
                eprintln!("{}", err);
            }
            Err(anyhow::anyhow!("Err when test nodes"))
        }
    }
}
