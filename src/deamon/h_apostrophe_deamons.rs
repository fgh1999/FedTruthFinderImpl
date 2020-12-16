use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonSet, GetFields};
use super::deamon_set::{DeamonOperation, DeamonOperations};
use super::super::id::Gid;
use sharks::{secret_type::Rational, Share};
use std::collections::HashMap;

/// H'(k) where `share.x` = k
type ResultType = Share<Rational>;

/// H(gid(u_{chosen}), k) where this slave's gid is $gid(u_{chosen})$, `share.x` is $k$
/// And this client only receive one kind of $k$
/// which also means that all the received `share.x` is the same(i.e., uid of this client).
/// The number of this kind of payloads received by each client should equals to group_num.
/// share.y.len() should be equal to client_num.
type ChannelPayload = (Gid, Share<Rational>);

pub type HApoDeamon = Deamon<ResultType, ChannelPayload>;
#[tonic::async_trait]
impl DeamonOperation<ResultType, ChannelPayload> for HApoDeamon {
    async fn process(&self) -> Result<ResultType, DeamonError> {
        let group_n = self.get_threshold() as usize; // 2t + 1, group_n
        let client_n = self.get_client_n() as usize;

        let rx_buffer = self.get_buffer();
        let mut buffer_lock = rx_buffer.lock().await;

        let mut buffer: HashMap<Gid, Share<Rational>> = HashMap::with_capacity(group_n);
        while buffer.len() < group_n {
            match buffer_lock.recv().await {
                Some((gid, share)) => {
                    buffer.insert(gid, share);
                    // TODO: check if inserted => check the return value of `insert`
                    // TODO: add check for consistency of `share.x` => add a new DeamonError
                }
                None => {
                    return Err(DeamonError::SharesNotEnough);
                }
            }
        }

        let y: Vec<_> = (0..client_n).into_iter().map(
            |i| buffer.iter().map(|(_gid, share)| share.y[i].clone()).sum()
        ).collect();

        let some_share: Vec<_> = buffer.values().take(1).collect();
        let some_share = some_share[0];
        Ok(Share { x: some_share.x, y })
    }
}

pub type HApoDeamonSet = DeamonSet<ResultType, ChannelPayload>;
impl DeamonOperations<ResultType, ChannelPayload> for HApoDeamonSet {}
