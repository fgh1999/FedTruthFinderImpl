tonic::include_proto!("algo");
use super::{
    deamon_set::{DeamonOperations, DeamonSet},
    deamon_error::DeamonError,
    event::{Eid, EidAssigner, Event, EventIdentifier, Judge},
    id::{Gid, Id, Uid, UidAssigner},
    leaderboard_deamons::*,
    summation_deamons::*,
    ResponseResult, ResponseStream,
};
use sharks::{secret_type::Rational, Share};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tokio::sync::{RwLock, mpsc};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct AlgoServer {
    shared: Arc<Shared>,
}

// #[derive(Debug)]
// struct InnerSelectionNotification(Eid, Gid, Uid);

#[derive(Debug)]
struct Shared {
    pub group_n: Gid, //number of groups

    pub uid_assigner: UidAssigner,
    pub clients: dashmap::DashMap<Uid, Arc<ClientInfo>>,
    pub groups: dashmap::DashMap<Gid, Vec<Arc<ClientInfo>>>,

    pub eid_assigner: EidAssigner,
    pub events: dashmap::DashMap<EventIdentifier, Event>,

    /// stores the confidence of events that already figured out
    pub event_confidence: dashmap::DashMap<Eid, f64>,

    /// stores the ascending leader board of clients' trustworthiness.
    /// `Eid` indicates if this leader board is latest enough
    pub leader_board: RwLock<(Eid, Vec<Uid>)>,

    event_computation_configurations: dashmap::DashMap<Eid, EventConfidenceComputationConfig>,
    trustworthiness_assessment_configurations: dashmap::DashMap<Eid, LeaderBoardComputationConfig>,
    trustworthiness_assessment_notifier: mpsc::UnboundedSender<Eid>,
    summation_deamons: SummationDeamonSet,
    leaderboard_deamons: LeaderBoardDeamonSet,
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub id: Id,
    pub pk: Vec<u8>,
    pub mailbox: SocketAddr,
}

impl Into<InitResponse> for Id {
    fn into(self) -> InitResponse {
        InitResponse {
            uid: self.get_uid(),
            gid: self.get_gid(),
        }
    }
}
impl Into<InitResponse> for ClientInfo {
    fn into(self) -> InitResponse {
        self.id.into()
    }
}

#[allow(dead_code)]
impl AlgoServer {
    pub fn new(group_n: Gid) -> Self {
        let (trustworthiness_assessment_notifier, mut rx) = mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            group_n,
            uid_assigner: UidAssigner::default(),
            clients: dashmap::DashMap::default(),
            groups: dashmap::DashMap::default(),

            eid_assigner: EidAssigner::default(),
            events: dashmap::DashMap::default(),

            event_confidence: dashmap::DashMap::default(),
            leader_board: RwLock::new((0 as Eid, Vec::new())),

            event_computation_configurations: dashmap::DashMap::default(),
            trustworthiness_assessment_configurations: dashmap::DashMap::default(),
            trustworthiness_assessment_notifier,

            summation_deamons: DeamonSet::new(),
            leaderboard_deamons: DeamonSet::new(),
        });

        // start a deamon for selections
        let shared_for_selection = shared.clone();
        tokio::spawn(async move {
            while let Some(eid) = rx.recv().await {
                let config = shared_for_selection.trustworthiness_assessment_configurations.get(&eid);
                if config.is_none() {
                    continue;
                }

                let client_num = config.unwrap().clients.len() as Uid;
                let group_num = shared_for_selection.group_n.clone();
                let mut handles = Vec::with_capacity(shared_for_selection.group_n as usize);
                for group in shared_for_selection.groups.iter() {
                    // select a client
                    let selected_client = selection_operations::select_from_group(group.value()).await;
                    if selected_client.is_none() {
                        continue;
                    }
                    let selected_client = selected_client.unwrap();

                    let handle = tokio::spawn(async move {
                        selection_operations::notify_to_group_share_r(eid, client_num, group_num, selected_client.clone()).await.unwrap();
                        selection_operations::notify_to_group_share_h_set(eid, client_num, group_num, selected_client).await.unwrap();
                    });
                    handles.push(handle);
                }
                futures::future::join_all(handles).await; // TODO: handle the result
            }
        });

        AlgoServer { shared }
    }

    pub fn summation_err_to_status(e: DeamonError) -> Status {
        match e {
            DeamonError::ThresholdInconsistency { given, existed } => Status::data_loss(format!(
                "threshold inconsistency: given {}, while the existed is {}",
                given, existed
            )),
            DeamonError::ClientNumInconsistency { given, existed } => Status::data_loss(format!(
                "client_n inconsistency: given {}, while the existed is {}",
                given, existed
            )),
            DeamonError::SharesNotEnough => Status::data_loss("shares not enough"),
            DeamonError::Timeout(_) => Status::deadline_exceeded("result calculation timeout"),
            DeamonError::InvalidConversion => Status::data_loss("result cannot be converted into f64"),
            DeamonError::SharesCannotAdded => {
                Status::data_loss("cannot add this share (should not happen)")
            }
            DeamonError::Unimplemented(detail) => Status::unimplemented(detail),
        }
    }
}

///Interfaces to manage clients
#[tonic::async_trait]
pub trait ClientManagement {
    /// New and return a unique uid
    fn new_uid(&self) -> Uid;

    /// Get the preset number of group
    fn get_group_n(&self) -> Gid;

    /// Add a new client with its public key and return it
    async fn add_client(&self, pk: &[u8], mailbox: &SocketAddr) -> Option<Arc<ClientInfo>>;

    /// Return a clone of query result
    async fn get_client_by_pk(&self, pk: &[u8]) -> Option<Arc<ClientInfo>>;

    /// Return a clone of query result
    async fn get_client_by_uid(&self, uid: &Uid) -> Option<Arc<ClientInfo>>;

    /// Return clones of clients which are of the given gid
    async fn get_clients_by_gid(&self, gid: &Gid) -> Vec<Arc<ClientInfo>>;

    /// Return clones of all clients
    async fn get_all_client(&self) -> Vec<Arc<ClientInfo>>;

    /// Return the number of clients
    fn get_client_n(&self) -> Uid;

    /// Return a copy of required Sockaddr if it exists
    async fn get_mailbox(&self, uid: &Uid) -> Option<SocketAddr>;

    /// Given a uid, result in a corresponding gid.
    /// This also means that gid only depends on uid and inner state of client manager.
    fn get_gid(&self, uid: &Uid) -> Gid;
}

#[tonic::async_trait]
impl ClientManagement for AlgoServer {
    fn new_uid(&self) -> Uid {
        let shared = self.shared.clone();
        shared.uid_assigner.new_uid()
    }

    fn get_group_n(&self) -> Gid {
        let shared = self.shared.clone();
        shared.group_n
    }

    async fn add_client(&self, pk: &[u8], mailbox: &SocketAddr) -> Option<Arc<ClientInfo>> {
        match self.get_client_by_pk(pk).await {
            Some(_) => None,
            None => {
                let uid = self.new_uid();
                let pk = pk.to_vec();
                let new_client = Arc::new(ClientInfo {
                    id: Id::new(uid, self.get_gid(&uid)),
                    pk,
                    mailbox: mailbox.clone(),
                });

                let shared = self.shared.clone();
                let res = match shared
                    .clone()
                    .clients
                    .insert(new_client.id.get_uid(), new_client.clone())
                {
                    Some(new_one) => Some(new_one),
                    None => None,
                };

                // update the groups
                shared.groups.entry(new_client.id.get_gid()).or_insert(Vec::new()).push(new_client);
                res
            }
        }
    }

    async fn get_client_by_pk(&self, pk: &[u8]) -> Option<Arc<ClientInfo>> {
        let pk = pk.to_vec();
        let shared = self.shared.clone();
        let target = shared.clients.iter().find(|x| x.value().pk == pk);
        match target {
            Some(target) => Some(target.value().clone()),
            None => None,
        }
    }

    async fn get_client_by_uid(&self, uid: &Uid) -> Option<Arc<ClientInfo>> {
        let shared = self.shared.clone();
        let target = shared.clients.get(uid);
        match target {
            Some(target) => Some(target.value().clone()),
            None => None,
        }
    }

    async fn get_mailbox(&self, uid: &Uid) -> Option<SocketAddr> {
        let shared = self.shared.clone();
        let target = shared.clients.get(uid);
        match target {
            Some(target) => Some(target.value().mailbox.clone()),
            None => None,
        }
    }

    async fn get_clients_by_gid(&self, gid: &Gid) -> Vec<Arc<ClientInfo>> {
        let mut targets = vec![];
        let shared = self.shared.clone();
        shared
            .clients
            .iter()
            .filter(|x| x.value().id.get_gid() == *gid)
            .for_each(|x| targets.push(x.value().clone()));

        targets
    }

    async fn get_all_client(&self) -> Vec<Arc<ClientInfo>> {
        let shared = self.shared.clone();
        // very costly
        shared
            .clients
            .iter()
            .map(|ref x| x.value().clone())
            .collect()
    }

    fn get_client_n(&self) -> Uid {
        self.shared.clients.iter().count() as Uid
    }

    fn get_gid(&self, uid: &Uid) -> Gid {
        *uid % self.shared.group_n
    }
}

const NO_NEED_TO_PUB_EMPTY_NOTIFICATION: &str = "There's no need to publish a empty notification";

#[tonic::async_trait]
impl algo_master_server::AlgoMaster for AlgoServer {
    async fn register(&self, request: Request<InitRequest>) -> ResponseResult<InitResponse> {
        // 通过（证书）验证请求方的身份
        let InitRequest {
            ref pk,
            ref mailbox,
        } = request.into_inner();
        let mailbox = mailbox.parse();
        match mailbox {
            Ok(mailbox) => match self.add_client(pk, &mailbox).await {
                Some(new_one) => Ok(Response::new(new_one.id.into())),
                None => Err(Status::already_exists("Public key has been registered")),
            },
            Err(e) => Err(tonic::Status::invalid_argument(format!(
                "Invalid mailbox addr: {}",
                e
            ))),
        }
    }

    type FetchClientPkStream = ResponseStream<PublicIdentity>;
    async fn fetch_client_pk(
        &self,
        req: Request<KeyRequest>,
    ) -> Result<Response<Self::FetchClientPkStream>, Status> {
        // 在 self.pk_clients_map 中 找到 符合req 的 client 信息，转换为Identity 生成器返回
        if let Some(request) = req.into_inner().request {
            let results = match request {
                key_request::Request::All(_) => self.get_all_client().await,
                key_request::Request::Uid(ref uid) => match self.get_client_by_uid(uid).await {
                    Some(client) => vec![client],
                    None => Vec::new(),
                },
            };
            let output = async_stream::try_stream! {
                for x in results.iter() {
                    yield PublicIdentity {
                        uid: x.id.get_uid(),
                        gid: x.id.get_gid(),
                        pk: x.pk.clone()
                    }
                }
            };
            Ok(Response::new(Box::pin(output) as Self::FetchClientPkStream))
        } else {
            Err(Status::invalid_argument("should use all or uid"))
        }
    }

    async fn publish_tau_sequence(&self, req: Request<TauSeqSharePubRequest>) -> ResponseResult<()> {
        let TauSeqSharePubRequest { topic_gid, notification } = req.into_inner();
        // TODO: check if notification.eid is valid
        // check if the notification is empty
        if notification.is_none() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let notification = notification.unwrap();
        if notification.share.is_empty() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }

        let subscribers = self.get_clients_by_gid(&topic_gid).await;
        let mut handles = Vec::with_capacity(subscribers.len());

        for subscriber in subscribers {
            let mailbox_url = format!("http://{}", subscriber.mailbox);
            let request = Request::new(notification.clone());

            let handle = tokio::spawn(async move {
                if let Ok(mut client) =
                    algo_node_client::AlgoNodeClient::connect(mailbox_url).await
                {
                    if let Err(_) = client.notify_tau_sequence(request).await {
                        Err(subscriber.id.get_uid())
                    } else {
                        Ok(())
                    }
                } else {
                    Err(subscriber.id.get_uid())
                }
            });

            handles.push(handle);
        }

        let mut failed_connections = vec![];
        for handle in handles {
            if let Err(uid) = handle.await {
                failed_connections.push(uid);
            }
        }

        if failed_connections.is_empty() {
            Ok(Response::new((())))
        } else {
            let mut list = String::new();
            failed_connections
                .iter()
                .for_each(|x| list.push_str(x.to_string().as_ref()));
            Err(Status::failed_precondition(format!(
                "Failed to publish to such client(s):{}",
                list
            )))
        }
    }

    async fn publish_r(&self, req: Request<RandCoefSharePubRequest>) -> ResponseResult<()> {
        let RandCoefSharePubRequest { topic_gid, notification } = req.into_inner();
        // TODO: check if notification.eid is valid
        // check if the notification is empty
        if notification.is_none() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let notification = notification.unwrap();
        if notification.share.is_empty() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }

        let subscribers = self.get_clients_by_gid(&topic_gid).await;
        let mut handles = Vec::with_capacity(subscribers.len());

        for subscriber in subscribers {
            let mailbox_url = format!("http://{}", subscriber.mailbox);
            let request = Request::new(notification.clone());

            let handle = tokio::spawn(async move {
                if let Ok(mut client) =
                    algo_node_client::AlgoNodeClient::connect(mailbox_url).await
                {
                    if let Err(_) = client.notify_r(request).await {
                        Err(subscriber.id.get_uid())
                    } else {
                        Ok(())
                    }
                } else {
                    Err(subscriber.id.get_uid())
                }
            });

            handles.push(handle);
        }

        let mut failed_connections = vec![];
        for handle in handles {
            if let Err(uid) = handle.await {
                failed_connections.push(uid);
            }
        }

        if failed_connections.is_empty() {
            Ok(Response::new((())))
        } else {
            let mut list = String::new();
            failed_connections
                .iter()
                .for_each(|x| list.push_str(x.to_string().as_ref()));
            Err(Status::failed_precondition(format!(
                "Failed to publish to such client(s):{}",
                list
            )))
        }
    }

    async fn forward_h_share(&self, req: Request<RelayMessage>) -> ResponseResult<()> {
        let message = req.into_inner();
        // check against this message:
        //  1. eid in range
        if !self
            .shared
            .event_computation_configurations
            .contains_key(&message.eid)
        {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                message.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if self
            .shared
            .event_computation_configurations
            .get(&message.eid)
            .unwrap()
            .value()
            .clients_pk
            .len()
            != message.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&message.rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = algo_node_client::AlgoNodeClient::connect(mailbox_uri).await {
                let forward_req = Request::new(message);
                client.send_h_share(forward_req).await
            } else {
                Err(Status::failed_precondition(format!(
                    "Client {} cannot be connected at this moment",
                    message.rx_uid
                )))
            }
        } else {
            Err(Status::not_found(format!(
                "can not find {}(uid)",
                message.rx_uid
            )))
        }
    }

    async fn forward(&self, request: Request<RelayMessage>) -> ResponseResult<()> {
        let message = request.into_inner();
        // check against this message:
        //  1. eid in range
        if !self
            .shared
            .event_computation_configurations
            .contains_key(&message.eid)
        {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                message.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if self
            .shared
            .event_computation_configurations
            .get(&message.eid)
            .unwrap()
            .value()
            .clients_pk
            .len()
            != message.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&message.rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = algo_node_client::AlgoNodeClient::connect(mailbox_uri).await {
                let forward_req = Request::new(message);
                client.forward(forward_req).await
            } else {
                Err(Status::failed_precondition(format!(
                    "Client {} cannot be connected at this moment",
                    message.rx_uid
                )))
            }
        } else {
            Err(Status::not_found(format!(
                "can not find {}(uid)",
                message.rx_uid
            )))
        }
    }

    async fn submit_summation(&self, req: Request<SubmitSummationRequest>) -> ResponseResult<f64> {
        let SubmitSummationRequest {
            eid,
            summation_pair,
            allowed_seconds,
        } = req.into_inner();

        let shared = self.shared.clone();
        // verify if the eid is valid, e.g. in range.
        if !shared.event_computation_configurations.contains_key(&eid) {
            return Err(Status::failed_precondition(format!(
                "No event computation configuration for event {}",
                eid
            )));
        }

        if let Some(confidence) = shared.event_confidence.get(&eid) {
            return Ok(Response::new(confidence.value().clone()));
        }

        use core::convert::TryFrom; // deserialize `summation_pair`
        match Share::<Rational>::try_from(&summation_pair[..]) {
            Ok(summation_pair) => {
                let configuration = shared.event_computation_configurations.get(&eid).unwrap();
                let threshold = configuration.value().threshold as u8;
                let client_n = configuration.value().clients_pk.len() as u8;
                drop(configuration);

                match shared
                    .summation_deamons
                    .add_share(&eid, summation_pair, threshold, client_n)
                    .await
                {
                    Ok(_) => {
                        let time_limit = Duration::from_secs_f64(allowed_seconds);
                        match shared
                            .summation_deamons
                            .get_result(&eid, threshold, client_n, time_limit)
                            .await
                        {
                            Ok(event_confidence) => {
                                // erase the buffered event_confidence_computation_configuration (eid)'s clients_pk
                                // ** should not erase the buffered event_confidence_computation_configuration (eid)
                                // ** cause we depends on it to verify the precondition
                                // let entry =
                                //     shared.event_computation_configurations.entry(eid.clone());
                                // hold the write lock before updating shared.event_confidence to avoid some sync problems

                                // update the confidence of this event
                                shared
                                    .event_confidence
                                    .insert(eid, event_confidence.clone());

                                // update after updating the confidence of event
                                // entry.and_modify(|config| config.clients_pk.clear());
                                // !! the erasing of corresponding shared.event_computation_configurations
                                // !! has been yeid to `submit_h_apo_share`

                                Ok(Response::new(event_confidence))
                            }
                            Err(e) => Err(AlgoServer::summation_err_to_status(e)),
                        }
                    }
                    Err(e) => Err(AlgoServer::summation_err_to_status(e)),
                }
            }
            Err(_) => Err(Status::invalid_argument(
                "Cannot deserialize Share<Rational> from SubmitSummationRequest.summation_pair",
            )),
        }
    }

    async fn submit_h_apo_share(&self, req: Request<HApoShare>) -> ResponseResult<LeaderBoard> {
        let HApoShare {
            eid,
            share,
            allowed_seconds,
        } = req.into_inner();
        let eid = eid as Eid;

        let shared = self.shared.clone();
        // verify if the eid is valid, e.g. in range.
        if !shared.event_computation_configurations.contains_key(&eid) {
            return Err(Status::failed_precondition(format!(
                "event {} is out of range",
                eid
            )));
        }

        let leader_board_r = shared.leader_board.read().await;
        if eid <= leader_board_r.0 {
            // request eid is out-of-date
            return Ok(Response::new(LeaderBoard {
                clients: leader_board_r.1.clone(),
            }));
        }
        drop(leader_board_r);

        use core::convert::TryFrom; // deserialize
        match Share::<Rational>::try_from(&share[..]) {
            Ok(share) => {
                let configuration = shared.event_computation_configurations.get(&eid).unwrap();
                let threshold = configuration.value().threshold as u8;
                let client_n = configuration.value().clients_pk.len() as u8;
                drop(configuration);

                match shared
                    .leaderboard_deamons
                    .add_share(&eid, share, threshold, client_n)
                    .await
                {
                    Ok(_) => {
                        let time_limit = Duration::from_secs_f64(allowed_seconds);
                        match shared
                            .leaderboard_deamons
                            .get_result(&eid, threshold, client_n, time_limit)
                            .await
                        {
                            Ok(leader_board) => {
                                // erase the buffered event_confidence_computation_configuration (eid)'s clients_pk
                                // ** should not erase the buffered event_confidence_computation_configuration (eid)
                                // ** cause we depends on it to verify preconditions
                                let entry =
                                    shared.event_computation_configurations.entry(eid.clone());
                                // hold the write lock before updating shared.event_confidence to avoid some sync problems

                                // update the leaderboard
                                let mut leader_board_w = shared.leader_board.write().await;
                                if eid > leader_board_w.0 {
                                    *leader_board_w = (eid, leader_board.clone());
                                }

                                // update after updating the confidence of event
                                entry.and_modify(|config| config.clients_pk.clear());

                                Ok(Response::new(LeaderBoard {
                                    clients: leader_board,
                                }))
                            }
                            Err(e) => Err(AlgoServer::summation_err_to_status(e)),
                        }
                    }
                    Err(e) => Err(AlgoServer::summation_err_to_status(e)),
                }
            }
            Err(_) => Err(Status::invalid_argument(
                "Cannot deserialize Share<Rational> from `HApoShare.share`",
            )),
        }
    }

    async fn get_group_num(&self, _req: Request<()>) -> ResponseResult<i32> {
        Ok(Response::new(self.get_group_n()))
    }

    async fn get_client_num(&self, _req: Request<()>) -> ResponseResult<i32> {
        Ok(Response::new(self.get_client_n()))
    }

    async fn get_event_confidence_computation_config(
        &self,
        req: Request<EventConfidenceComputationConfigRequest>,
    ) -> ResponseResult<EventConfidenceComputationConfig> {
        // if not existed => create and insert a new one; otherwise, return the buffered one.
        let eid: Eid = req.into_inner().eid;
        let entry = self
            .shared
            .event_computation_configurations
            .entry(eid)
            .or_insert({
                // do a snapshot and buffer it (identified by eid)
                let clients = self.get_all_client().await;
                use std::collections::HashMap;
                let clients_pk: HashMap<Uid, Vec<u8>> = clients
                    .iter()
                    .map(|x| (x.id.get_uid(), x.pk.clone()))
                    .collect();
                let threshold = (clients_pk.len() / 2) as i32; // TODO: hard-coded here at this moment
                EventConfidenceComputationConfig {
                    threshold,
                    clients_pk,
                }
            });
        Ok(Response::new(entry.value().clone()))
    }

    async fn find_or_register_event(
        &self,
        req: Request<EventRegistrationRequest>,
    ) -> ResponseResult<EventRegistrationResponse> {
        let event_identifier: EventIdentifier = req.into_inner().identifier;
        let eid = match self.shared.events.get(&event_identifier) {
            Some(target) => {
                // this event has already existed on the server => return its eid
                target.value().get_id()
            }
            None => {
                // this is a new event => add it & return its eid
                let id = self.shared.eid_assigner.new_id();
                self.shared
                    .events
                    .insert(event_identifier, Event::new(id, Judge::True));
                id
            }
        };
        Ok(Response::new(EventRegistrationResponse { eid }))
    }

    async fn get_leader_board_computation_config(
        &self,
        req: Request<LeaderBoardComputationConfigRequest>,
    ) -> ResponseResult<LeaderBoardComputationConfig> {
        let eid = req.into_inner().eid;
        let entry = self
            .shared.trustworthiness_assessment_configurations
            .entry(eid)
            .or_insert({
                let event_confidence_computaion_configuration = self.shared.event_computation_configurations.get(&eid);
                if let None = event_confidence_computaion_configuration {
                    return Err(Status::failed_precondition(format!(
                        "event(eid={})'s confidence hasn't been calculated",
                        eid
                    )));
                }
                let config = event_confidence_computaion_configuration.unwrap();
                let clients_uids_iter = config.clients_pk.keys();
                let mut clients = HashMap::new();
                clients.extend(clients_uids_iter.map(|uid| (*uid, self.get_gid(uid))));

                // notify to start selection
                self.shared.trustworthiness_assessment_notifier.send(eid).unwrap(); 
                LeaderBoardComputationConfig { clients }
            });
        Ok(Response::new(entry.value().clone()))
    }
}

pub mod selection_operations {
    use super::{Arc, ClientInfo, Eid, Uid, Gid, Selection, algo_node_client, Request};
    pub async fn select_from_group(clients: &[Arc<ClientInfo>]) -> Option<Arc<ClientInfo>> {
        if clients.is_empty() {
            None
        } else {
            Some(clients[0].clone())
        }
    }

    /// client_num should be consistent with the one in event_confidence_computation_config
    pub async fn notify_to_group_share_r(eid: Eid, client_num: Uid, group_num: Gid, selected_client: Arc<ClientInfo>) -> anyhow::Result<()> {
        let req = Request::new(Selection {
            eid, client_num, group_num
        });
        let mailbox_uri = format!("http://{}", selected_client.mailbox);
        let mut client = algo_node_client::AlgoNodeClient::connect(mailbox_uri).await?;
        client.select_to_share_r(req).await?;
        Ok(())
    }

    pub async fn notify_to_group_share_h_set(eid: Eid, client_num: Uid, group_num: Gid, selected_client: Arc<ClientInfo>) -> anyhow::Result<()> {
        let req = Request::new(Selection {
            eid, client_num, group_num
        });
        let mailbox_uri = format!("http://{}", selected_client.mailbox);
        let mut client = algo_node_client::AlgoNodeClient::connect(mailbox_uri).await?;
        client.select_to_share_h_set(req).await?;
        Ok(())
    }
}
