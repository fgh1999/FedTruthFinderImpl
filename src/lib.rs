pub mod config;
mod deamon;
pub mod event;
pub mod id;
pub mod master_server;
pub mod slave_server;

use futures::Stream;
use std::pin::Pin;
use tonic::{Response, Status};
type ResponseResult<T> = Result<Response<T>, Status>;
type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

pub fn fmt_leader_board(leader_board: Vec<id::Uid>) -> String {
    use std::fmt::Write as FmtWrite;
    let mut content = String::new();
    for rank in leader_board {
        write!(&mut content, " {}", rank).unwrap();
    }
    content
}
