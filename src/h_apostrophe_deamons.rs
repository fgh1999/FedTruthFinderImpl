use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonSet, GetFields};
use super::deamon_set::{DeamonOperation, DeamonOperations};
use sharks::{secret_type::Rational, Share};

type ResultType = Share<Rational>; // H'(k) where `share.x` = k

// H(gid(u_{chosen}), k) where `Gid` is $gid(u_{chosen})$, `share.x` is $k$
// And this client only receive one kind of $k$
// which also means that all the received `share.x` is the same(i.e., uid of this client)
type ChannelPayload = Share<Rational>;

pub type HApoDeamon = Deamon<ResultType, ChannelPayload>;
#[tonic::async_trait]
impl DeamonOperation<ResultType, ChannelPayload> for HApoDeamon {
    async fn process(&self) -> Result<ResultType, DeamonError> {
        let threshold = self.get_threshold(); // t + 1
        let group_n = (threshold - 1) * 2 + 1;
        let client_n = self.get_client_n();

        let rx_buffer = self.get_buffer();
        let mut buffer_lock = rx_buffer.lock().await;

        let buffer_size = group_n as usize;
        let mut buffer = Vec::with_capacity(buffer_size);
        while buffer.len() < buffer_size {
            match buffer_lock.recv().await {
                Some(payload) => {
                    // every payload is a Share{ x: k, y: disturbed H'(k)} where k is the uid of this client
                    buffer.push(payload); // TODO: add check for consistency of `share.x` => add a new DeamonError
                }
                None => {
                    return Err(DeamonError::SharesNotEnough);
                }
            }
        }

        let mut y = Vec::with_capacity(client_n as usize);
        for i in 0..client_n as usize {
            let y_i = buffer.iter().map(|share| share.y[i].clone()).sum();
            y.push(y_i);
        }
        Ok(Share { x: buffer[0].x, y })
    }
}

pub type HApoDeamonSet = DeamonSet<ResultType, ChannelPayload>;
impl DeamonOperations<ResultType, ChannelPayload> for HApoDeamonSet {}
