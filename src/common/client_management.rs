tonic::include_proto!("client");
use futures::Stream;
use std::{pin::Pin, vec::Vec};
use tonic::{Request, Response, Status};
use parking_lot::RwLock;

type ResponseResult<T> = Result<Response<T>, Status>;
type ResponseStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;
type Uid = i32;
type Gid = i32;

#[derive(Default, Debug, Clone)]
pub struct Client {
    pub uid: Uid,
    pub gid: Gid,
    pub pk: Vec<u8>,
}

#[derive(Default)]
pub struct ClientManager {
    group_n: Gid,
    clients: RwLock<dashmap::DashMap<Vec<u8>, Client>>,
}

///Interfaces to manage clients
pub trait ClientManagement {
    ///Get the preset number of group
    fn get_group_n(&self) -> Gid;

    ///Add a new client with its public key and return it
    fn add_client(&self, pk: &[u8]) -> Client;

    ///return a clone of query result
    fn get_client_by_pk(&self, pk: &[u8]) -> Option<Client>;

    ///return a clone of query result
    fn get_client_by_uid(&self, uid: Uid) -> Option<Client>;

    ///return clones of all clients
    fn get_all_client(&self) -> Vec<Client>;

    ///Given a uid, result in a corresponding gid.
    ///This also means that gid only depends on uid and inner state of client manager.
    fn get_gid(&self, uid: Uid) -> Gid;
}

impl ClientManagement for ClientManager {
    fn get_group_n(&self) -> Gid {
        self.group_n
    }

    fn add_client(&self, pk: &[u8]) -> Client {
        let pk = pk.to_vec();

        let w_clients = self.clients.write();
        let uid = w_clients.len() as i32;
        let new_client = Client {
            uid,
            gid: self.get_gid(uid),
            pk
        };
        w_clients.insert(new_client.pk.clone(), new_client.clone());
        new_client
    }

    fn get_client_by_pk(&self, pk: &[u8]) -> Option<Client> {
        let r_clients = self.clients.read();
        let target = r_clients.get(&pk.to_vec());
        match target {
            Some(target) => Some(target.clone()),
            None => None
        }
    }

    fn get_client_by_uid(&self, uid: Uid) -> Option<Client> {
        let r_clients = self.clients.read();
        let target = r_clients.iter().find(|x| x.value().uid == uid);
        match target {
            Some(target) => Some(target.clone()),
            None => None
        }
    }

    fn get_all_client(&self) -> Vec<Client> {
        let r_clients = self.clients.read();
        r_clients.iter().map(|x| x.value().clone()).collect()
    }

    fn get_gid(&self, uid: Uid) -> Gid {
        uid % self.group_n
    }
}

#[tonic::async_trait]
impl client_management_server::ClientManagement for ClientManager {
    ///init a new identity according to the given pk; otherwise, return its corresponding identity
    async fn get_identity(&self, request: Request<PublicKey>) -> ResponseResult<Identity> {
        // 通过（证书）验证请求方的身份
        let ref pk = request.into_inner().pk;
        Ok(Response::new(
            if let Some(existed_client) = self.get_client_by_pk(pk) {
                Identity {
                    uid: existed_client.uid,
                    gid: existed_client.gid,
                    pk: existed_client.pk
                }
            } else {
                let new_client = self.add_client(pk);
                Identity {
                    uid: new_client.uid,
                    gid: new_client.gid,
                    pk: new_client.pk
                }
            }
        ))
    }

    type FetchClientPkStream = ResponseStream<Identity>;
    async fn fetch_client_pk(
        &self,
        req: Request<KeyRequest>
    ) -> Result<Response<Self::FetchClientPkStream>, Status> {
        // 在 self.clients 中 找到 符合req 的 client 信息，转换为Identity 生成器返回
        if let Some(request) = req.into_inner().request {
            let results = match request {
                key_request::Request::All(_) => self.get_all_client(),
                key_request::Request::Uid(uid) => {
                    match self.get_client_by_uid(uid) {
                        Some(client) => vec![client],
                        None => Vec::new()
                    }
                }
            };
            let output = async_stream::try_stream! {
                for x in results.into_iter().map(|x| x.into()) {
                    yield x;
                }
            };
            Ok(Response::new(Box::pin(output) as Self::FetchClientPkStream))
        } else {
            Err(Status::invalid_argument("should use all or uid"))
        }
    }
}

impl Into<Client> for Identity {
    fn into(self) -> Client {
        Client {
            uid: self.uid,
            gid: self.gid,
            pk: self.pk
        }
    }
}

impl Into<Identity> for Client {
    fn into(self) -> Identity {
        Identity {
            uid: self.uid,
            gid: self.gid,
            pk: self.pk
        }
    }
}