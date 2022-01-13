mod algo_util;
mod client_management;
pub mod config;
pub mod event;
pub mod id;
pub mod master_server;
pub mod slave_server;

use chrono::Utc;
use psutil::network;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// use futures::Stream;
// use std::pin::Pin;
use tonic::{Response, Status};
type ResponseResult<T> = Result<Response<T>, Status>;
// type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

pub fn fmt_leader_board(leader_board: Vec<id::Uid>) -> String {
    use std::fmt::Write as FmtWrite;
    let mut content = String::new();
    for rank in leader_board {
        write!(&mut content, " {}", rank).unwrap();
    }
    content
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct NetIfInfo {
    isup: bool,
    duplex: u8,
    mtu: u64,
}

impl From<&network::NetIfStats> for NetIfInfo {
    fn from(stats: &network::NetIfStats) -> Self {
        let duplex: u8 = match stats.duplex() {
            network::Duplex::Full => 2,
            network::Duplex::Half => 1,
            network::Duplex::Unknown => 0,
        };
        Self {
            isup: stats.is_up(),
            duplex,
            mtu: stats.mtu(),
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct NetIfStatus {
    cur_time: u128,
    bytes_sent: u64,
    bytes_recv: u64,
    packets_sent: u64,
    packets_recv: u64,
    errin: u64,
    errout: u64,
    dropin: u64,
    dropout: u64,
}

impl NetIfStatus {
    fn from_counters(counters: &network::NetIoCounters, time_ms: u128) -> Self {
        Self {
            cur_time: time_ms,
            bytes_recv: counters.bytes_recv(),
            bytes_sent: counters.bytes_sent(),
            packets_recv: counters.packets_recv(),
            packets_sent: counters.packets_sent(),
            errin: counters.err_in(),
            errout: counters.err_out(),
            dropin: counters.drop_in(),
            dropout: counters.drop_out(),
        }
    }
}

impl From<&network::NetIoCounters> for NetIfStatus {
    fn from(counters: &network::NetIoCounters) -> Self {
        Self {
            cur_time: Utc::now().timestamp_millis() as u128,
            bytes_recv: counters.bytes_recv(),
            bytes_sent: counters.bytes_sent(),
            packets_recv: counters.packets_recv(),
            packets_sent: counters.packets_sent(),
            errin: counters.err_in(),
            errout: counters.err_out(),
            dropin: counters.drop_in(),
            dropout: counters.drop_out(),
        }
    }
}

pub async fn snapshot_net_io() -> HashMap<String, NetIfStatus> {
    let mut collector = network::NetIoCountersCollector::default();
    let time = Utc::now().timestamp_millis() as u128;
    collector
        .net_io_counters_pernic()
        .unwrap()
        .into_iter()
        .map(|(name, counters)| (name, NetIfStatus::from_counters(&counters, time)))
        .collect::<HashMap<_, _>>()
}

pub async fn get_network_total_io_bytes() -> (u64, u64) {
    let snapshot = snapshot_net_io().await;
    let i_bytes = snapshot.values().map(|net_stat| net_stat.bytes_recv).sum();
    let o_bytes = snapshot.values().map(|net_stat| net_stat.bytes_sent).sum();
    (i_bytes, o_bytes)
}
