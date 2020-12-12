use sharks::{secret_type::Rational, Share};
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, RwLock};

use super::deamon_error::DeamonError;
use crate::{event::Eid, id::Uid};

type Payload = Share<Rational>;
type Threshold = u8;

#[derive(Debug)]
struct ForwardDeamon {
    eid: Eid,             // which event this deamon is responsible for
    threshold: Threshold, // how many shares needed to figure out the summation result => should be equal to client_n
    client_n: Uid,
    share_entry_tx: Arc<RwLock<Option<mpsc::Sender<Payload>>>>, // dropped in get_summation_result as quickly as possible
    result_tx: Arc<broadcast::Sender<Payload>>,
    result: Arc<RwLock<Option<Payload>>>,
}

impl ForwardDeamon {
    pub fn new(eid: Eid, threshold: Threshold, client_n: Uid) -> ForwardDeamon {
        let (share_entry_tx, mut share_entry_rx) = mpsc::channel::<Payload>(client_n as usize);
        let (result_tx, _) = broadcast::channel(4);

        let result_tx = Arc::new(result_tx);
        let result_tx_ = result_tx.clone();
        tokio::spawn(async move {
            let mut needed_shares: Vec<Payload> = Vec::with_capacity(threshold as usize);

            while let Some(new_share) = share_entry_rx.recv().await {
                // TODO: handle the exception when new_share is `None`
                needed_shares.push(new_share);
                if needed_shares.len() == threshold as usize {
                    break;
                }
            }

            let needed_shares = needed_shares;
            // TODO: handle the exception that shares in the channel isn't enough for needed_shares
            // but share_entry_rx.recv().await returns None

            let d_jk: Rational = needed_shares.iter().map(|share| share.y[0].clone()).sum();
            let s_jk = needed_shares.iter().map(|share| share.y[1].clone()).sum();
            let summation_result = Share {
                x: needed_shares[0].x,
                y: vec![d_jk, s_jk],
            };
            // make sure that at least one client received this result
            while let Err(_) = result_tx_.send(summation_result.clone()) {}

            while let Some(_) = share_entry_rx.recv().await {} // clear the rest remained in the channel
        });

        ForwardDeamon {
            eid,
            threshold,
            client_n,
            result: Arc::new(RwLock::new(None)),
            result_tx,
            share_entry_tx: Arc::new(RwLock::new(Some(share_entry_tx))),
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
    async fn add_received_share(&self, share: Share<Rational>) -> Result<(), DeamonError>;

    async fn get_summation_result(&self) -> Share<Rational>;
}

#[tonic::async_trait]
impl SummationOperation for ForwardDeamon {
    async fn get_summation_result(&self) -> Share<Rational> {
        let result_r = self.result.read().await;
        if result_r.is_some() {
            result_r.as_ref().unwrap().clone()
        } else {
            drop(result_r);
            let mut result_w = self.result.write().await;
            if result_w.is_none() {
                let summation_result = self.result_tx.subscribe().recv().await;
                let summation_result = summation_result.unwrap(); // TODO: handle this possible error
                *result_w = Some(summation_result.clone());
                let mut share_entry_tx_w = self.share_entry_tx.write().await;
                *share_entry_tx_w = None; // drop it in order to end the background inner server

                summation_result
            } else {
                result_w.as_ref().unwrap().clone()
            }
        }
    }

    async fn add_received_share(&self, share: Share<Rational>) -> Result<(), DeamonError> {
        let share_entry_tx_r = self.share_entry_tx.read().await;
        if share_entry_tx_r.is_some() {
            drop(share_entry_tx_r);
            let mut share_entry_tx_w = self.share_entry_tx.write().await;
            let tx = share_entry_tx_w.as_mut().unwrap();
            match tx.send(share).await {
                Ok(_) => Ok(()),
                Err(_) => Err(DeamonError::SharesCannotAdded),
            }
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct ForwardDeamonSet {
    // deamons: dashmap::DashMap<Eid, ForwardDeamon>,
    deamons: RwLock<HashMap<Eid, ForwardDeamon>>,
}

impl ForwardDeamonSet {
    pub fn new() -> ForwardDeamonSet {
        ForwardDeamonSet {
            // deamons: dashmap::DashMap::new(),
            deamons: RwLock::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
pub trait SummationOperations {
    async fn add_received_share(
        &self,
        eid: &Eid,
        share: Share<Rational>,
        client_n: u8,
    ) -> Result<(), DeamonError>;

    async fn get_summation_result(
        &self,
        eid: &Eid,
        client_n: u8,
    ) -> Result<Share<Rational>, DeamonError>;
}

#[tonic::async_trait]
impl SummationOperations for ForwardDeamonSet {
    async fn add_received_share(
        &self,
        eid: &Eid,
        share: Share<Rational>,
        client_n: u8,
    ) -> Result<(), DeamonError> {
        let threshold = client_n;
        let client_n = client_n as Uid;

        if !self.deamons.read().await.contains_key(eid) {
            let mut deamons_w = self.deamons.write().await;
            deamons_w.entry(eid.clone()).or_insert(ForwardDeamon::new(
                eid.clone(),
                threshold,
                client_n,
            ));
        }
        let deamons_r = self.deamons.read().await;
        let deamon = deamons_r.get(eid).unwrap();

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, deamon.get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, deamon.get_client_n())?;

        deamon.add_received_share(share).await
    }

    async fn get_summation_result(
        &self,
        eid: &Eid,
        client_n: u8,
    ) -> Result<Share<Rational>, DeamonError> {
        let threshold = client_n;
        let client_n = client_n as Uid;
        if !self.deamons.read().await.contains_key(eid) {
            let mut deamons_w = self.deamons.write().await;
            deamons_w.entry(eid.clone()).or_insert(ForwardDeamon::new(
                eid.clone(),
                threshold,
                client_n,
            ));
        }
        let deamons_r = self.deamons.read().await;
        let deamon = deamons_r.get(eid).unwrap();

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, deamon.get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, deamon.get_client_n())?;

        Ok(deamon.get_summation_result().await)
    }
}
