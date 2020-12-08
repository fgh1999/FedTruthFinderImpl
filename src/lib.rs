pub mod algo_client;
pub mod algo_server;
pub mod config;
pub mod event;
pub mod id;

pub mod deamon_error;
pub mod deamon_set;
pub mod forward_deamons;
pub mod h_apostrophe_deamons;
pub mod h_deamons;
pub mod leaderboard_deamons;
pub mod summation_deamons;

use futures::Stream;
use std::pin::Pin;
use tonic::{Response, Status};
pub type ResponseResult<T> = Result<Response<T>, Status>;
pub type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

#[cfg(test)]
mod test {
    use super::*;
    
    fn test_timer_wraper() {
        use std::time;
        let start = time::Instant::now();
        // TODO: main test case
        let end = time::Instant::now();
        let duration = end - start;
        println!("time cost: {:?} s", duration.as_secs_f64());
    }
}
