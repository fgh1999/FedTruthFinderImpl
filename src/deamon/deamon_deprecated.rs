use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::Mutex;
use tonic::Status;

use std::ops::Fn;
use std::sync::{self, Arc};

type ArcMutex<T> = Arc<sync::Mutex<T>>;

#[derive(Debug)]
pub struct Deamon<P, R>
where
    P: Send + Sync,
    R: Send + Sync,
{
    param_tx: Mutex<Sender<P>>,
    result_rx: Mutex<Receiver<R>>,
    shutdown: ArcMutex<bool>,
}

#[allow(dead_code)]
impl<P, R> Deamon<P, R>
where
    P: Send + Sync + 'static,
    R: 'static + Send + Sync + Default,
{
    /// new and start a deamon.
    pub fn new<F>(channel_size: usize, process: F) -> Deamon<P, R>
    where
        F: Sync + Send + 'static + Fn(P) -> R,
    {
        let (param_tx, mut param_rx) = channel(channel_size);
        let (mut result_tx, result_rx) = channel(channel_size);

        let shutdown_identifier = sync::Arc::new(sync::Mutex::new(false));
        let shutdown_identifier_ = shutdown_identifier.clone();
        tokio::spawn(async move {
            while *shutdown_identifier_.lock().unwrap() {
                if let Some(param) = param_rx.recv().await {
                    result_tx
                        .send(process(param))
                        .await
                        .unwrap_or_else(|_| eprintln!("channel closed (result_tx in deamon)"));
                }
            }
        });

        Deamon {
            param_tx: Mutex::new(param_tx),
            result_rx: Mutex::new(result_rx),
            shutdown: shutdown_identifier.clone(),
        }
    }
}

#[tonic::async_trait]
pub trait DeamonInterface<P, R>
where
    P: Send + Sync,
    R: Send + Sync,
{
    /// An interop with the constructed deamon.
    /// This will `await` until the result is returned from the deamon
    async fn process(&self, param: P) -> Result<R, Status>;

    /// If the deamon had been shutdown
    fn is_shutdown(&self) -> bool;
}

#[tonic::async_trait]
impl<P, R> DeamonInterface<P, R> for Deamon<P, R>
where
    P: Send + Sync,
    R: Send + Sync,
{
    async fn process(&self, param: P) -> Result<R, Status> {
        let mut snd = self.param_tx.lock().await;
        let mut rev = self.result_rx.lock().await;
        // process after await **both** mutex
        match snd.send(param).await {
            Ok(_) => {
                if let Some(res) = rev.recv().await {
                    Ok(res)
                } else {
                    Err(Status::failed_precondition(
                        "Background deamon has been shut down",
                    ))
                }
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    fn is_shutdown(&self) -> bool {
        *self.shutdown.lock().unwrap()
    }
}
