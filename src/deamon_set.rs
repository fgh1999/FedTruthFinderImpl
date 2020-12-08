use super::{deamon_error::DeamonError, event::Eid, id::Uid};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, RwLock};

type Threshold = u8;

#[derive(Debug)]
pub struct Deamon<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
> {
    eid: Eid,
    result: Arc<RwLock<Option<ResultType>>>,
    threshold: Threshold,
    client_n: Uid,
    tx: Arc<RwLock<Option<mpsc::Sender<ChannelPayload>>>>,
    buffer: Arc<Mutex<mpsc::Receiver<ChannelPayload>>>,
}

impl<ResultType: Clone + Send + Sync + 'static, ChannelPayload: Clone + Send + Sync + 'static>
    Deamon<ResultType, ChannelPayload>
{
    pub fn new(eid: Eid, threshold: Threshold, client_n: Uid) -> Self {
        let (tx, rx) = mpsc::channel(client_n as usize * 2); // TODO: set an appropriate buffer size
        let tx = Arc::new(RwLock::new(Some(tx)));

        Self {
            result: Arc::new(RwLock::new(None)),
            threshold,
            client_n,
            eid,
            tx,
            buffer: Arc::new(Mutex::new(rx)),
        }
    }
}

pub trait GetFields<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
>
{
    fn get_threshold(&self) -> Threshold;
    fn get_client_n(&self) -> Uid;
    fn get_inner_result(&self) -> Arc<RwLock<Option<ResultType>>>;
    fn get_eid(&self) -> Eid;
    fn get_tx(&self) -> Arc<RwLock<Option<mpsc::Sender<ChannelPayload>>>>;
    fn get_buffer(&self) -> Arc<Mutex<mpsc::Receiver<ChannelPayload>>>;
}

impl<ResultType: Clone + Send + Sync + 'static, ChannelPayload: Clone + Send + Sync + 'static>
    GetFields<ResultType, ChannelPayload> for Deamon<ResultType, ChannelPayload>
{
    fn get_threshold(&self) -> Threshold {
        self.threshold
    }
    fn get_client_n(&self) -> Uid {
        self.client_n
    }
    fn get_inner_result(&self) -> Arc<RwLock<Option<ResultType>>> {
        self.result.clone()
    }
    fn get_eid(&self) -> Eid {
        self.eid
    }
    fn get_tx(&self) -> Arc<RwLock<Option<mpsc::Sender<ChannelPayload>>>> {
        self.tx.clone()
    }
    fn get_buffer(&self) -> Arc<Mutex<mpsc::Receiver<ChannelPayload>>> {
        self.buffer.clone()
    }
}

#[tonic::async_trait]
pub trait DeamonOperation<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
>: GetFields<ResultType, ChannelPayload>
{
    async fn add_share(&self, payload: ChannelPayload) -> Result<(), DeamonError> {
        let tx = self.get_tx();
        let tx_r = tx.read().await;
        if tx_r.is_some() {
            let mut tx = tx_r.as_ref().unwrap().clone();
            match tx.send(payload).await {
                Ok(_) => Ok(()),
                Err(_) => Err(DeamonError::SharesCannotAdded), // ReceiveHalfClosed
            }
        } else {
            Ok(())
        }
    }

    async fn get_result(&self, time_limit: Duration) -> Result<ResultType, DeamonError> {
        let inner_result = self.get_inner_result();
        let result_r = inner_result.read().await;
        if result_r.is_none() {
            drop(result_r);
            let mut result_w = inner_result.write().await;
            // check again in case of computing for serveral times
            if result_w.is_none() {
                // code in this scope will only perform once
                match tokio::time::timeout(time_limit, self.process()).await {
                    // with timeout
                    Ok(result) => match result {
                        Ok(result) => {
                            *result_w = Some(result.clone()); // update the inner result
                            let tx = self.get_tx();
                            let mut tx_w = tx.write().await;
                            *tx_w = None; // drop self.tx

                            let buffer = self.get_buffer();
                            let rx = buffer.clone();
                            tokio::spawn(async move {
                                // clear the rest in the channel
                                let mut rx = rx.lock().await;
                                while let Some(_) = rx.recv().await {}
                            }); // no need to await this
                            Ok(result)
                        }
                        Err(e) => Err(e),
                    },
                    Err(e) => Err(DeamonError::from(e)),
                }
            } else {
                Ok(result_w.as_ref().unwrap().clone())
            }
        } else {
            Ok(result_r.as_ref().unwrap().clone())
        }
    }

    // should only perform once
    async fn process(&self) -> Result<ResultType, DeamonError> {
        Err(DeamonError::Unimplemented(String::from(
            "DeamonOperation<ResultType>::process is unimplemented",
        )))
    }
}

#[derive(Debug)]
pub struct DeamonSet<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
> where
    Deamon<ResultType, ChannelPayload>: DeamonOperation<ResultType, ChannelPayload>,
{
    deamons: Arc<dashmap::DashMap<Eid, Deamon<ResultType, ChannelPayload>>>,
}

pub trait GetSetFields<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
>
{
    fn get_deamons(&self) -> Arc<dashmap::DashMap<Eid, Deamon<ResultType, ChannelPayload>>>;
}
impl<ResultType: Clone + Send + Sync + 'static, ChannelPayload: Clone + Send + Sync + 'static>
    GetSetFields<ResultType, ChannelPayload> for DeamonSet<ResultType, ChannelPayload>
where
    Deamon<ResultType, ChannelPayload>: DeamonOperation<ResultType, ChannelPayload>,
{
    fn get_deamons(&self) -> Arc<dashmap::DashMap<Eid, Deamon<ResultType, ChannelPayload>>> {
        self.deamons.clone()
    }
}

impl<ResultType: Clone + Send + Sync + 'static, ChannelPayload: Clone + Send + Sync + 'static>
    DeamonSet<ResultType, ChannelPayload>
where
    Deamon<ResultType, ChannelPayload>: DeamonOperation<ResultType, ChannelPayload>,
{
    pub fn new() -> Self {
        DeamonSet {
            deamons: Arc::new(dashmap::DashMap::new()),
        }
    }
}

#[tonic::async_trait]
pub trait DeamonOperations<
    ResultType: Clone + Send + Sync + 'static,
    ChannelPayload: Clone + Send + Sync + 'static,
>: GetSetFields<ResultType, ChannelPayload> where
    Deamon<ResultType, ChannelPayload>: DeamonOperation<ResultType, ChannelPayload>,
{
    async fn add_share(
        &self,
        eid: &Eid,
        payload: ChannelPayload,
        threshold: u8,
        client_n: u8,
    ) -> Result<(), DeamonError> {
        let client_n = client_n as Uid;
        let deamons = self.get_deamons();
        let entry =
            deamons
                .entry(eid.clone())
                .or_insert(Deamon::new(eid.clone(), threshold, client_n));

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, entry.value().get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, entry.value().get_client_n())?;

        entry.value().add_share(payload).await
    }

    async fn get_result(
        &self,
        eid: &Eid,
        threshold: u8,
        client_n: u8,
        time_limitation: Duration,
    ) -> Result<ResultType, DeamonError> {
        let client_n = client_n as Uid;
        let deamons = self.get_deamons();
        let entry =
            deamons
                .entry(eid.clone())
                .or_insert(Deamon::new(eid.clone(), threshold, client_n));

        // check consistency of (threshold, client_n)
        DeamonError::check_threshold_consistency(threshold, entry.value().get_threshold())?;
        DeamonError::check_client_num_consistency(client_n, entry.value().get_client_n())?;

        entry.value().get_result(time_limitation).await
    }
}
