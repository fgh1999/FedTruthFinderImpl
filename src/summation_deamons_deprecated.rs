use super::{
    deamon_error::DeamonError,
    event::Eid,
    id::Uid,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};

use sharks::{secret_type::Rational, Share, Sharks};

type Threshold = u8;
type RestoredResult = f64;

#[derive(Debug)]
struct SummationDeamon {
    eid: Eid,
    result: RwLock<Option<RestoredResult>>,
    threshold: Threshold,
    client_n: Uid,
    tx: RwLock<Option<mpsc::Sender<Share<Rational>>>>,
    buffer: Arc<Mutex<mpsc::Receiver<Share<Rational>>>>,
}

impl SummationDeamon {
    pub fn new(eid: Eid, threshold: Threshold, client_n: Uid) -> SummationDeamon {
        let (tx, rx) = mpsc::channel(client_n as usize);
        let tx = RwLock::new(Some(tx));

        SummationDeamon {
            result: RwLock::new(None),
            threshold,
            client_n,
            eid,
            tx,
            buffer: Arc::new(Mutex::new(rx)),
        }
    }

    pub fn get_threshold(&self) -> Threshold {
        self.threshold
    }

    pub fn get_client_n(&self) -> Uid {
        self.client_n
    }
}

#[tonic::async_trait]
trait SummationOperation {
    async fn add_summation_shares(&self, share: Share<Rational>) -> Result<(), DeamonError>;

    async fn get_result(&self, time_limit: Duration) -> Result<RestoredResult, DeamonError>;
}

#[tonic::async_trait]
impl SummationOperation for SummationDeamon {
    async fn add_summation_shares(&self, share: Share<Rational>) -> Result<(), DeamonError> {
        let tx_r = self.tx.read().await;
        if tx_r.is_some() {
            let mut tx = tx_r.as_ref().unwrap().clone();
            match tx.send(share).await {
                Ok(_) => Ok(()),
                Err(e) => Err(DeamonError::from(e)), // ReceiveHalfClosed
            }
        } else {
            Ok(())
        }
    }

    async fn get_result(&self, time_limit: Duration) -> Result<RestoredResult, DeamonError> {
        let result_r = self.result.read().await;
        if result_r.is_none() {
            drop(result_r);
            let mut result_w = self.result.write().await;
            // check again in case of computing for serveral times
            if result_w.is_none() {
                // code in this scope will only perform once
                let rx_buffer = self.buffer.clone();
                let threshold = self.threshold.clone();

                let calculation = async move {
                    let mut buffer: Vec<Share<Rational>> = Vec::with_capacity(threshold as usize);
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
                };

                // with timeout
                match tokio::time::timeout(time_limit, calculation).await {
                    Ok(result) => {
                        match result {
                            Ok(result) => {
                                *result_w = Some(result.clone()); // update the inner result
                                let mut tx_w = self.tx.write().await;
                                *tx_w = None; // drop self.tx

                                let rx = self.buffer.clone();
                                tokio::spawn(async move {
                                    // clear the rest in the channel
                                    let mut rx = rx.lock().await;
                                    while let Some(_) = rx.recv().await {}
                                }); // no need to await this
                                Ok(result)
                            }
                            Err(e) => Err(e),
                        }
                    }
                    Err(e) => Err(DeamonError::from(e)),
                }
            } else {
                Ok(result_w.unwrap().clone())
            }
        } else {
            Ok(result_r.unwrap().clone())
        }
    }
}

#[derive(Debug, Default)]
pub struct SummationDeamonSet {
    deamons: dashmap::DashMap<Eid, SummationDeamon>,
}

#[tonic::async_trait]
pub trait SummationOperations {
    async fn add_share(
        &self,
        eid: &Eid,
        share: Share<Rational>,
        threshold: u8,
        client_n: u8,
    ) -> Result<(), DeamonError>;

    async fn get_result(
        &self,
        eid: &Eid,
        threshold: u8,
        client_n: u8,
        time_limit: Duration,
    ) -> Result<RestoredResult, DeamonError>;
}

#[tonic::async_trait]
impl SummationOperations for SummationDeamonSet {
    async fn add_share(
        &self,
        eid: &Eid,
        share: Share<Rational>,
        threshold: u8,
        client_n: u8,
    ) -> Result<(), DeamonError> {
        let client_n = client_n as Uid;
        let entry = self
            .deamons
            .entry(eid.clone())
            .or_insert(SummationDeamon::new(eid.clone(), threshold, client_n));

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, entry.value().get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, entry.value().get_client_n())?;

        entry.value().add_summation_shares(share).await
    }

    async fn get_result(
        &self,
        eid: &Eid,
        threshold: u8,
        client_n: u8,
        time_limit: Duration,
    ) -> Result<RestoredResult, DeamonError> {
        let client_n = client_n as Uid;
        let entry = self
            .deamons
            .entry(eid.clone())
            .or_insert(SummationDeamon::new(eid.clone(), threshold, client_n));

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, entry.value().get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, entry.value().get_client_n())?;

        entry.value().get_result(time_limit).await
    }
}
