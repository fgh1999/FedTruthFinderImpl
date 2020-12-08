use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonSet, GetFields};
use super::deamon_set::{DeamonOperation, DeamonOperations};
use sharks::{secret_type::Rational, Share, Sharks};

type PayloadType = Share<Rational>;
type SummationDeamon = Deamon<f64, PayloadType>;

#[tonic::async_trait]
impl DeamonOperation<f64, PayloadType> for SummationDeamon {
    async fn process(&self) -> Result<f64, DeamonError> {
        let threshold = self.get_threshold();
        let mut buffer: Vec<Share<Rational>> = Vec::with_capacity(threshold as usize);
        let rx_buffer = self.get_buffer();
        let mut buffer_lock = rx_buffer.lock().await;
        while buffer.len() < threshold as usize {
            match buffer_lock.recv().await {
                Some(new_share) => {
                    buffer.push(new_share);
                }
                None => {
                    return Err::<f64, DeamonError>(DeamonError::SharesNotEnough);
                }
            }
        }

        let mut shark = Sharks::new();
        shark.threshold = threshold;
        use sharks::SecretSharingOperation;
        match shark.recover(buffer) {
            Ok(secret_res) => {
                let sum_d = secret_res[0].clone();
                let sum_s = secret_res[1].clone();
                let rho_j = sum_d / sum_s;

                use num_traits::ToPrimitive;
                match rho_j.to_f64() {
                    Some(rho_j) => Ok(rho_j),
                    None => Err(DeamonError::InvalidConversion),
                }
            }
            Err(_) => Err(DeamonError::SharesNotEnough), // shares not enough
        }
    }
}

pub type SummationDeamonSet = DeamonSet<f64, PayloadType>;
impl DeamonOperations<f64, PayloadType> for SummationDeamonSet {}
