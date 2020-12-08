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
