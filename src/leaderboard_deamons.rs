use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonOperation, DeamonOperations, DeamonSet, GetFields};
use super::id::Uid;
use sharks::{secret_type::Rational, SecretSharingOperation, Share, Sharks};

type ChannelPayload = Share<Rational>; // (k, H'(k)) where h'_i(k) are ordered by i // deprecated this comment
type ResultType = Vec<Uid>; // ordered by their corresponding restored h_i' ascendingly

pub type LeaderBoardDeamon = Deamon<ResultType, ChannelPayload>;

#[tonic::async_trait]
impl DeamonOperation<ResultType, ChannelPayload> for LeaderBoardDeamon {
    async fn process(&self) -> Result<ResultType, DeamonError> {
        let threshold = self.get_threshold(); // t + 1
        let rx_buffer = self.get_buffer();

        let mut buffer = Vec::with_capacity(threshold as usize);
        let mut buffer_lock = rx_buffer.lock().await;
        while buffer.len() < threshold as usize {
            match buffer_lock.recv().await {
                Some(payload) => {
                    // every payload is a Share{ x: k, y: disturbed H'(k)} where k \in 1..=client_n
                    buffer.push(payload);
                }
                None => {
                    return Err::<ResultType, DeamonError>(DeamonError::SharesNotEnough);
                }
            }
        }

        let mut shark = Sharks::new();
        shark.threshold = threshold;
        match shark.recover(buffer) {
            Ok(h_set) => {
                let mut temp: Vec<_> = h_set.iter().zip(1..=h_set.len() as Uid).collect();
                temp.sort_unstable_by_key(|pair| pair.0);
                Ok(temp.into_iter().map(|pair| pair.1).collect())
            }
            Err(_) => Err(DeamonError::SharesNotEnough), // shares not enough
        }
    }
}

pub type LeaderBoardDeamonSet = DeamonSet<ResultType, ChannelPayload>;
impl DeamonOperations<ResultType, ChannelPayload> for LeaderBoardDeamonSet {}
