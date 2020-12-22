tonic::include_proto!("algo");
use super::{
    event::{Eid, EidAssigner, Event, EventIdentifier, Judge},
    fmt_leader_board,
    id::{Gid, Id, Uid, UidAssigner},
    ResponseResult,
};
use super::deamon::{
    deamon_error::DeamonError,
    deamon_set::{DeamonOperations, DeamonSet},
    leaderboard_deamons::*,
    summation_deamons::*,

};
use sharks::{secret_type::Rational, Share};
use slog::{crit, debug, error, info, trace, warn, Logger};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tonic::{Request, Response, Status};

/// A master server instance
#[derive(Debug)]
pub struct MasterServer {
    shared: Arc<Shared>,
}

/// Shared fields among requests
#[derive(Debug)]
struct Shared {
    logger: Logger,
    pub group_n: Gid, //number of groups

    pub uid_assigner: UidAssigner,
    pub clients: RwLock<HashMap<Uid, Arc<SlaveInfo>>>,
    pub groups: RwLock<HashMap<Gid, Vec<Arc<SlaveInfo>>>>,

    pub eid_assigner: EidAssigner,
    pub events: RwLock<HashMap<EventIdentifier, Event>>,

    /// stores the confidence of events that already figured out
    pub event_confidence: RwLock<HashMap<Eid, f64>>,

    /// stores the ascending leader board of clients' trustworthiness.
    /// `Eid` indicates if this leader board is latest enough
    pub leader_board: RwLock<(Eid, Vec<Uid>)>,

    event_computation_configurations: RwLock<HashMap<Eid, EventConfidenceComputationConfig>>,
    trustworthiness_assessment_configurations: RwLock<HashMap<Eid, LeaderBoardComputationConfig>>,
    trustworthiness_assessment_notifier: mpsc::UnboundedSender<Eid>,
    summation_deamons: SummationDeamonSet,
    leaderboard_deamons: LeaderBoardDeamonSet,
}

/// A struct of slave inforamtion on the master side
#[derive(Debug, Clone)]
pub struct SlaveInfo {
    pub id: Id,
    pub pk: Vec<u8>,
    pub mailbox: SocketAddr,
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
            "SlaveInfo {{ id: {}, pk: {}, mailbox: {} }}",
            self.id, pk, self.mailbox
        )
    }
}

impl Into<InitResponse> for Id {
    fn into(self) -> InitResponse {
        InitResponse {
            uid: self.get_uid(),
            gid: self.get_gid(),
        }
    }
}
impl Into<InitResponse> for SlaveInfo {
    fn into(self) -> InitResponse {
        self.id.into()
    }
}


impl MasterServer {
    pub fn new<L: Into<Logger>>(group_n: Gid, logger: L) -> Self {
        let (trustworthiness_assessment_notifier, mut rx) = mpsc::unbounded_channel();
        let logger = logger.into();

        let shared = Arc::new(Shared {
            logger: logger.clone(),
            group_n,
            uid_assigner: UidAssigner::default(),
            clients: RwLock::new(HashMap::new()),
            groups: RwLock::new(HashMap::new()),

            eid_assigner: EidAssigner::default(),
            events: RwLock::new(HashMap::new()),

            event_confidence: RwLock::new(HashMap::new()),
            leader_board: RwLock::new((0 as Eid, Vec::new())),

            event_computation_configurations: RwLock::new(HashMap::new()),
            trustworthiness_assessment_configurations: RwLock::new(HashMap::new()),
            trustworthiness_assessment_notifier,

            summation_deamons: DeamonSet::new(),
            leaderboard_deamons: DeamonSet::new(),
        });

        // start a deamon for selections
        let shared_for_selection = shared.clone();
        let logger4deamon = logger;
        tokio::spawn(async move {
            while let Some(eid) = rx.recv().await {
                let configurations_r = shared_for_selection
                    .trustworthiness_assessment_configurations
                    .read()
                    .await;
                let configuration = configurations_r.get(&eid);
                if configuration.is_none() {
                    continue;
                }

                let client_num = configuration.unwrap().clients.len() as Uid;
                drop(configurations_r);
                let group_num = shared_for_selection.group_n.clone();
                let mut handles = Vec::with_capacity(shared_for_selection.group_n as usize);
                let groups_r = shared_for_selection.groups.read().await;
                for group in groups_r.values() {
                    // select a client
                    let selected_client =
                        selection_operations::select_from_group(group).await;
                    if selected_client.is_none() {
                        continue;
                    }
                    let selected_client = selected_client.unwrap();

                    let logger = logger4deamon.clone();
                    let handle = tokio::spawn(async move {
                        if let Err(e) = selection_operations::notify_to_group_share_r(
                            eid,
                            client_num,
                            group_num,
                            selected_client.clone(),
                        )
                        .await
                        {
                            crit!(logger, #"selection", "failed to select to share r group-wisely";
                                "eid" => eid,
                                "err" => %e,
                            );
                        } else {
                            info!(logger, #"selection", "select to share r group-wisely";
                                "eid" => eid,
                                "selected_uid" => selected_client.id.get_uid(),
                            );
                        }

                        if let Err(e) = selection_operations::notify_to_group_share_h_set(
                            eid,
                            client_num,
                            group_num,
                            selected_client.clone(),
                        )
                        .await
                        {
                            crit!(logger, #"selection", "failed to select to share h set group-wisely";
                                "eid" => eid,
                                "err" => %e,
                            );
                        } else {
                            info!(logger, #"selection", "select to share h set group-wisely";
                                "eid" => eid,
                                "selected_uid" => selected_client.id.get_uid(),
                            );
                        }
                    });
                    handles.push(handle);
                }
                futures::future::join_all(handles).await; // TODO: handle the result
            }
        });

        MasterServer { shared }
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
            DeamonError::InvalidConversion => {
                Status::data_loss("result cannot be converted into f64")
            }
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
    async fn add_client(&self, pk: &[u8], mailbox: &SocketAddr) -> Option<Arc<SlaveInfo>>;

    /// Return a clone of query result
    async fn get_client_by_pk(&self, pk: &[u8]) -> Option<Arc<SlaveInfo>>;

    /// Return a clone of query result
    async fn get_client_by_uid(&self, uid: &Uid) -> Option<Arc<SlaveInfo>>;

    /// Return clones of clients which are of the given gid
    async fn get_clients_by_gid(&self, gid: &Gid) -> Vec<Arc<SlaveInfo>>;

    /// Return clones of all clients
    async fn get_all_client(&self) -> Vec<Arc<SlaveInfo>>;

    /// Return a copy of required Sockaddr if it exists
    async fn get_mailbox(&self, uid: &Uid) -> Option<SocketAddr>;

    /// Given a uid, result in a corresponding gid.
    /// This also means that gid only depends on uid and inner state of client manager.
    fn get_gid(&self, uid: &Uid) -> Gid;
}

#[tonic::async_trait]
impl ClientManagement for MasterServer {
    fn new_uid(&self) -> Uid {
        let shared = self.shared.clone();
        shared.uid_assigner.new_uid()
    }

    fn get_group_n(&self) -> Gid {
        let shared = self.shared.clone();
        shared.group_n
    }

    async fn add_client(&self, pk: &[u8], mailbox: &SocketAddr) -> Option<Arc<SlaveInfo>> {
        match self.get_client_by_pk(pk).await {
            None => {
                let uid = self.new_uid();
                let pk = pk.to_vec();
                let new_client = Arc::new(SlaveInfo {
                    id: Id::new(uid, self.get_gid(&uid)),
                    pk,
                    mailbox: mailbox.clone(),
                });

                let mut clients_w = self.shared.clients.write().await;
                let mut groups_w = self.shared.groups.write().await;
                clients_w.insert(new_client.id.get_uid(), new_client.clone());

                // update the groups
                let client_gid = new_client.id.get_gid();
                if !groups_w.contains_key(&client_gid) {
                    groups_w.insert(client_gid.clone(), Vec::new());
                }
                groups_w.get_mut(&client_gid).unwrap().push(new_client.clone());

                Some(new_client)
            }
            Some(_) => None,
        }
    }

    async fn get_client_by_pk(&self, pk: &[u8]) -> Option<Arc<SlaveInfo>> {
        let pk = pk.to_vec();
        let clients_r = self.shared.clients.read().await;
        let target = clients_r.values().find(|x| x.pk == pk);
        match target {
            Some(target) => Some(target.clone()),
            None => None,
        }
    }

    async fn get_client_by_uid(&self, uid: &Uid) -> Option<Arc<SlaveInfo>> {
        let clients_r = self.shared.clients.read().await;
        let target = clients_r.get(uid);
        match target {
            Some(target) => Some(target.clone()),
            None => None,
        }
    }

    async fn get_mailbox(&self, uid: &Uid) -> Option<SocketAddr> {
        let clients_r = self.shared.clients.read().await;
        let target = clients_r.get(uid);
        match target {
            Some(target) => Some(target.mailbox.clone()),
            None => None,
        }
    }

    async fn get_clients_by_gid(&self, gid: &Gid) -> Vec<Arc<SlaveInfo>> {
        let mut targets = vec![];
        let clients_r = self.shared.clients.read().await;
        clients_r
            .values()
            .filter(|x| x.id.get_gid() == *gid)
            .for_each(|x| targets.push(x.clone()));

        targets
    }

    async fn get_all_client(&self) -> Vec<Arc<SlaveInfo>> {
        let clients_r = self.shared.clients.read().await;
        clients_r.values().cloned().collect()
    }

    #[inline]
    fn get_gid(&self, uid: &Uid) -> Gid {
        let gid = *uid % self.shared.group_n;
        if gid == 0 {
            self.shared.group_n
        } else {
            gid
        }
    }
}


const NO_NEED_TO_PUB_EMPTY_NOTIFICATION: &str = "There's no need to publish a empty notification";

#[tonic::async_trait]
impl master_server::Master for MasterServer {
    async fn register(&self, request: Request<InitRequest>) -> ResponseResult<InitResponse> {
        let InitRequest { pk, mailbox } = request.into_inner();
        let mailbox = mailbox.parse();
        match mailbox {
            Ok(mailbox) => match self.add_client(&pk, &mailbox).await {
                Some(new_one) => {
                    let all_clinets_n = self.get_all_client().await.len(); // TODO: provide a independent interface for count clients
                    info!(self.shared.logger, "registration";
                        "new client" => %new_one,
                        "clients_n" => all_clinets_n,
                    );
                    Ok(Response::new(new_one.id.into()))
                }
                None => Err(Status::already_exists("Public key has been registered")),
            },
            Err(e) => Err(tonic::Status::invalid_argument(format!(
                "Invalid mailbox addr: {}",
                e
            ))),
        }
    }

    async fn publish_tau_sequence(
        &self,
        req: Request<TauSeqSharePubRequest>,
    ) -> ResponseResult<()> {
        let TauSeqSharePubRequest {
            tx_uid: _,
            rx_uid,
            topic_gid: _,
            notification,
        } = req.into_inner();

        // check against this notification:
        //  0. check if the notification is empty
        if notification.is_none() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let notification = notification.unwrap();
        if notification.share.is_empty() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let configurations = self.shared.event_computation_configurations.read().await;
        //  1. eid in range
        if !configurations.contains_key(&notification.eid) {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                notification.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if configurations
            .get(&notification.eid)
            .unwrap()
            .clients_pk
            .len()
            != notification.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }
        drop(configurations);

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = slave_client::SlaveClient::connect(mailbox_uri).await {
                client.notify_tau_sequence(notification).await
            } else {
                Err(Status::failed_precondition(format!(
                    "Client {} cannot be connected at this moment",
                    rx_uid
                )))
            }
        } else {
            Err(Status::not_found(format!("can not find {}(uid)", rx_uid)))
        }
    }

    async fn publish_r(&self, req: Request<RandCoefSharePubRequest>) -> ResponseResult<()> {
        let RandCoefSharePubRequest {
            tx_uid: _,
            rx_uid,
            topic_gid: _,
            notification,
        } = req.into_inner();

        // check against this notification:
        //  0. check if the notification is empty
        if notification.is_none() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let notification = notification.unwrap();
        if notification.share.is_empty() {
            return Err(Status::invalid_argument(NO_NEED_TO_PUB_EMPTY_NOTIFICATION));
        }
        let configurations = self.shared.event_computation_configurations.read().await;
        //  1. eid in range
        if !configurations.contains_key(&notification.eid) {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                notification.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if configurations
            .get(&notification.eid)
            .unwrap()
            .clients_pk
            .len()
            != notification.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }
        drop(configurations);

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = slave_client::SlaveClient::connect(mailbox_uri).await {
                client.notify_r(notification).await
            } else {
                Err(Status::failed_precondition(format!(
                    "Client {} cannot be connected at this moment",
                    rx_uid
                )))
            }
        } else {
            Err(Status::not_found(format!("can not find {}(uid)", rx_uid)))
        }
    }

    async fn forward_h_share(&self, req: Request<HSetShareMessage>) -> ResponseResult<()> {
        let message = req.into_inner();

        // check against this message:
        let configurations = self.shared.event_computation_configurations.read().await;
        //  1. eid in range
        if !configurations.contains_key(&message.eid) {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                message.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if configurations.get(&message.eid).unwrap().clients_pk.len() != message.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }
        drop(configurations);

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&message.rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = slave_client::SlaveClient::connect(mailbox_uri).await {
                client.send_h_share(message).await
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
        let configurations = self.shared.event_computation_configurations.read().await;
        //  1. eid in range
        if !configurations.contains_key(&message.eid) {
            return Err(Status::failed_precondition(format!(
                "Event {} has not done `get_event_confidence_configuration`",
                message.eid
            )));
        }
        //  2. client_num == client_num(buffered on server)
        if configurations.get(&message.eid).unwrap().clients_pk.len() != message.client_num as usize
        {
            return Err(Status::invalid_argument(
                "Wrong .client_num, mismatched with the one buffered on server",
            ));
        }
        drop(configurations);

        // connect & forward the message to it destination('rx_uid')
        if let Some(dst) = self.get_client_by_uid(&message.rx_uid).await {
            let mailbox_uri = format!("http://{}", dst.mailbox);
            if let Ok(mut client) = slave_client::SlaveClient::connect(mailbox_uri).await {
                match client.forward(message.clone()).await {
                    Ok(ok) => {
                        trace!(self.shared.logger, #"forward", "message forward successed";
                            "from_uid" => message.tx_uid,
                            "to_uid" => message.rx_uid,
                            "eid" => message.eid,
                            "client_num" => message.client_num,
                        );
                        Ok(ok)
                    }
                    Err(e) => {
                        error!(self.shared.logger, #"forward", "failed to forward message";
                            "from_uid" => message.tx_uid,
                            "to_uid" => message.rx_uid,
                            "eid" => message.eid,
                            "err" => %e,
                        );
                        Err(e)
                    }
                }
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
        if !shared
            .event_computation_configurations
            .read()
            .await
            .contains_key(&eid)
        {
            return Err(Status::failed_precondition(format!(
                "No event computation configuration for event {}",
                eid
            )));
        }

        if let Some(confidence) = shared.event_confidence.read().await.get(&eid) {
            return Ok(Response::new(confidence.clone()));
        }

        use core::convert::TryFrom; // deserialize `summation_pair`
        match Share::<Rational>::try_from(&summation_pair[..]) {
            Ok(summation_pair) => {
                let configurations = shared.event_computation_configurations.read().await;
                let configuration = configurations.get(&eid).unwrap();
                let threshold = configuration.threshold as u8;
                let client_n = configuration.clients_pk.len() as u8;
                drop(configurations);

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
                                    .write()
                                    .await
                                    .insert(eid, event_confidence);

                                // update after updating the confidence of event
                                // entry.and_modify(|config| config.clients_pk.clear());
                                // !! the erasing of corresponding shared.event_computation_configurations
                                // !! has been yeid to `submit_h_apo_share`

                                info!(self.shared.logger, "New event confidence";
                                    "eid" => eid, "confidence" => event_confidence);
                                Ok(Response::new(event_confidence))
                            }
                            Err(e) => Err(MasterServer::summation_err_to_status(e)),
                        }
                    }
                    Err(e) => Err(MasterServer::summation_err_to_status(e)),
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
        if !shared
            .event_computation_configurations
            .read()
            .await
            .contains_key(&eid)
        {
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
                let configurations = shared.event_computation_configurations.read().await;
                let configuration = configurations.get(&eid).unwrap();
                let client_n = configuration.clients_pk.len() as u8;
                drop(configurations);
                let threshold = (shared.group_n as u8 - 1) / 2 + 1;

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
                                // let mut config_w =
                                    // shared.event_computation_configurations.write().await;
                                // hold the write lock before updating shared.event_confidence to avoid some sync problems

                                // update the leaderboard
                                let mut leader_board_w = shared.leader_board.write().await;
                                if eid > leader_board_w.0 {
                                    *leader_board_w = (eid, leader_board.clone());
                                }

                                // update after updating the confidence of event
                                // config_w.get_mut(&eid).unwrap().clients_pk.clear();

                                info!(self.shared.logger, "Leader board";
                                    "eid"=> eid, "ascending uid ranking list" => fmt_leader_board(leader_board.clone())
                                );
                                Ok(Response::new(LeaderBoard {
                                    clients: leader_board,
                                }))
                            }
                            Err(e) => Err(MasterServer::summation_err_to_status(e)),
                        }
                    }
                    Err(e) => Err(MasterServer::summation_err_to_status(e)),
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

    async fn get_event_confidence_computation_config(
        &self,
        req: Request<EventConfidenceComputationConfigRequest>,
    ) -> ResponseResult<EventConfidenceComputationConfig> {
        // if not existed => create and insert a new one; otherwise, return the buffered one.
        let eid: Eid = req.into_inner().eid;

        // do a snapshot and buffer it (identified by eid)
        let config_r = self.shared.event_computation_configurations.read().await;
        if !config_r.contains_key(&eid) {
            drop(config_r);
            let mut config_w = self.shared.event_computation_configurations.write().await;
            if !config_w.contains_key(&eid) {
                config_w.insert(eid.clone(), {
                    let clients = self.get_all_client().await;
                    // while clients.len() < 3 {
                    //     clients = self.get_all_client().await;
                    // } // require at least 3 client TODO
                    use std::collections::HashMap;
                    let clients_pk: HashMap<Uid, Vec<u8>> = clients
                        .iter()
                        .map(|x| (x.id.get_uid(), x.pk.clone()))
                        .collect();
                    let threshold = (clients_pk.len() / 2) as i32; // TODO: hard-coded here at this moment
                    trace!(self.shared.logger, "new event confidence computation config";
                        "eid" => eid,
                        "clients_n" => clients.len(),
                        "threshold" => threshold,
                    );
                    EventConfidenceComputationConfig {
                        threshold,
                        clients_pk,
                    }
                });
            }
        }
        let config = self
            .shared
            .event_computation_configurations
            .read()
            .await
            .get(&eid)
            .unwrap()
            .clone();
        Ok(Response::new(config))
    }

    async fn find_or_register_event(
        &self,
        req: Request<EventRegistrationRequest>,
    ) -> ResponseResult<EventRegistrationResponse> {
        let event_identifier: EventIdentifier = req.into_inner().identifier;

        let events_r = self.shared.events.read().await;
        if !events_r.contains_key(&event_identifier) {
            drop(events_r);
            let mut events_w = self.shared.events.write().await;
            if !events_w.contains_key(&event_identifier) {
                events_w.insert(event_identifier.clone(), {
                    let id = self.shared.eid_assigner.new_id();
                    info!(self.shared.logger, #"event", "registed a new event";
                        "identifier" => &event_identifier,
                        "eid" => id,
                    );
                    Event::new(id, Judge::True)
                });
            }
        }
        let eid = self
            .shared
            .events
            .read()
            .await
            .get(&event_identifier)
            .unwrap()
            .get_id();
        Ok(Response::new(EventRegistrationResponse { eid }))
    }

    async fn get_leader_board_computation_config(
        &self,
        req: Request<LeaderBoardComputationConfigRequest>,
    ) -> ResponseResult<LeaderBoardComputationConfig> {
        let eid = req.into_inner().eid;

        let config_r = self
            .shared
            .trustworthiness_assessment_configurations
            .read()
            .await;
        if !config_r.contains_key(&eid) {
            drop(config_r);
            let mut config_w = self
                .shared
                .trustworthiness_assessment_configurations
                .write()
                .await;
            if !config_w.contains_key(&eid) {
                config_w.insert(eid, {
                    let configurations_r =
                        self.shared.event_computation_configurations.read().await;
                    let event_confidence_computaion_configuration = configurations_r.get(&eid);
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
                    drop(configurations_r);

                    // notify to start selection
                    self.shared
                        .trustworthiness_assessment_notifier
                        .send(eid)
                        .unwrap();
                    LeaderBoardComputationConfig { clients }
                });
            }
        }
        let config = self
            .shared
            .trustworthiness_assessment_configurations
            .read()
            .await
            .get(&eid)
            .unwrap()
            .clone();
        Ok(Response::new(config))
    }
}


pub mod selection_operations {
    use super::{slave_client, Arc, SlaveInfo, Eid, Gid, Request, Selection, Uid};

    /// select one client from the given list
    pub async fn select_from_group(clients: &[Arc<SlaveInfo>]) -> Option<Arc<SlaveInfo>> {
        if clients.is_empty() {
            None
        } else {
            Some(clients[0].clone())
        }
    }

    /// client_num should be consistent with the one in event_confidence_computation_config
    pub async fn notify_to_group_share_r(
        eid: Eid,
        client_num: Uid,
        group_num: Gid,
        selected_client: Arc<SlaveInfo>,
    ) -> anyhow::Result<()> {
        let req = Request::new(Selection {
            eid,
            client_num,
            group_num,
        });
        let mailbox_uri = format!("http://{}", selected_client.mailbox);
        let mut client = slave_client::SlaveClient::connect(mailbox_uri).await?;
        client.select_to_share_r(req).await?;
        Ok(())
    }

    pub async fn notify_to_group_share_h_set(
        eid: Eid,
        client_num: Uid,
        group_num: Gid,
        selected_client: Arc<SlaveInfo>,
    ) -> anyhow::Result<()> {
        let req = Selection {
            eid,
            client_num,
            group_num,
        };
        let mailbox_uri = format!("http://{}", selected_client.mailbox);
        let mut client = slave_client::SlaveClient::connect(mailbox_uri).await?;
        client.select_to_share_h_set(req).await?;
        Ok(())
    }
}
