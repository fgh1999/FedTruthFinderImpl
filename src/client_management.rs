use super::{
    event::Eid,
    id::{Gid, Uid},
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;

/// A struct of slave inforamtion on the master side
#[derive(Debug, Clone)]
pub struct SlaveInfo {
    pub uid: Uid,
    pub pk: Vec<u8>,
    pub mailbox: SocketAddr,
    pub identifier_map: HashMap<String, Eid>,
    pub online: Arc<RwLock<bool>>,
}
impl std::fmt::Display for SlaveInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut pk = String::new();
        self.pk
            .iter()
            .map(|byte| byte.to_string())
            .for_each(|byte| pk.push_str(byte.as_ref()));
        write!(
            f,
            "SlaveInfo {{ uid: {}, mailbox: {} }}",
            self.uid, self.mailbox
        )
    }
}

/// Interfaces to manage clients
#[tonic::async_trait]
pub trait ClientManagement {
    /// New and return a unique uid
    fn new_uid(&self) -> Uid;

    /// Get the preset number of group
    fn get_group_n(&self) -> Gid;

    /// Add a new client with its public key and return it
    async fn add_client(
        &self,
        pk: &[u8],
        mailbox: &SocketAddr,
        identifiers: Vec<String>,
    ) -> Option<Arc<SlaveInfo>>;

    /// Return a clone of query result
    async fn get_client_by_pk(&self, pk: &[u8]) -> Option<Arc<SlaveInfo>>;

    /// Return a clone of query result
    async fn get_client_by_uid(&self, uid: &Uid) -> Option<Arc<SlaveInfo>>;

    /// Return clones of all clients
    async fn get_all_clients(&self) -> Vec<Arc<SlaveInfo>>;

    /// Return a copy of required Sockaddr if it exists
    async fn get_mailbox(&self, uid: &Uid) -> Option<SocketAddr>;

    async fn get_online_clients(&self) -> Vec<Arc<SlaveInfo>>;
}
