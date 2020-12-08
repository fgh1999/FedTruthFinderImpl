tonic::include_proto!("algo");
use super::{
    config::ClientConfig,
    deamon_set::{DeamonOperations, DeamonSet},
    event::{Eid, Event, EventIdentifierReceiver, Judge},
    forward_deamons::{ForwardDeamonSet, SummationOperations},
    h_apostrophe_deamons::HApoDeamonSet,
    h_deamons::{HChannelPayload, HDeamonSet},
    id::{Gid, Id, Uid},
    ResponseResult,
};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use num_rational::BigRational;
use num_traits::cast::{FromPrimitive, ToPrimitive};

use ecies_ed25519::{self as ecies, PublicKey, SecretKey};
use sharks::{secret_type::Rational, Share};
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct AlgoClient {
    config: ClientConfig,
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    id: Id,
    sk: SecretKey,
    core: Mutex<AlgoCore>,

    /// for event confidence computation
    forward_deamons: ForwardDeamonSet,

    /// for PP trustworthiness assessment
    h_deamons: HDeamonSet,
    h_apostrophe_deamons: HApoDeamonSet,
}

#[derive(Debug)]
struct AlgoCore {
    pub events: Vec<Event>,
    pub tau: f64,
}

#[allow(dead_code)]
impl AlgoCore {
    /// Construct a new AlgoCore with a default tau = 0.5
    pub fn new() -> Self {
        AlgoCore {
            events: vec![],
            tau: 0.5,
        }
    }

    /// Construct a new AlgoCore with a given tau
    pub fn with_tau(tau: f64) -> Self {
        AlgoCore {
            events: vec![],
            tau,
        }
    }
}

#[tonic::async_trait]
impl algo_node_server::AlgoNode for AlgoClient {
    async fn notify_tau_sequence(
        &self,
        req: Request<TauSeqShareNotification>,
    ) -> ResponseResult<()> {
        let TauSeqShareNotification {
            eid,
            share,
            uid,
            group_num,
            client_num,
        } = req.into_inner();
        match Share::<Rational>::try_from(&share[..]) {
            Ok(share) => {
                let payload = HChannelPayload::TauSequenceShare(uid, share);
                match self
                    .shared
                    .h_deamons
                    .add_share(&eid, payload, group_num as u8, client_num as u8)
                    .await
                {
                    Ok(_) => Ok(Response::new((()))),
                    Err(e) => Err(Status::internal(format!("cannot share tau due to {}", e))),
                }
            }
            Err(_) => Err(Status::invalid_argument(
                "cannot deserialize a Share<Rational> from the given bytes",
            )),
        }
    }

    async fn notify_r(&self, req: Request<RandCoefShareNotification>) -> ResponseResult<()> {
        let RandCoefShareNotification {
            eid,
            share,
            gid,
            group_num,
            client_num,
        } = req.into_inner();
        match Share::<Rational>::try_from(&share[..]) {
            Ok(share) => {
                let payload = HChannelPayload::RandomCoefficientShare(gid, share);
                match self
                    .shared
                    .h_deamons
                    .add_share(&eid, payload, group_num as u8, client_num as u8)
                    .await
                {
                    Ok(_) => Ok(Response::new((()))),
                    Err(e) => Err(Status::internal(format!("cannot share tau due to {}", e))),
                }
            }
            Err(_) => Err(Status::invalid_argument(
                "cannot deserialize a Share<Rational> from the given bytes",
            )),
        }
    }

    async fn send_h_share(&self, req: Request<RelayMessage>) -> ResponseResult<()> {
        let msg = req.into_inner();
        let shared = self.shared.clone();
        // check rx_uid
        let uid = self.shared.id.get_uid();
        if msg.rx_uid != uid {
            return Err(Status::invalid_argument(format!(
                "rx_uid is {}, while this is client {}",
                msg.rx_uid, uid
            )));
        }

        let eid = msg.eid;
        let client_n = msg.client_num as u8;
        let msg = msg.encrypted_message;
        use std::convert::TryFrom;
        match ecies::decrypt(&self.shared.sk, &msg) {
            Ok(msg) => match Share::<Rational>::try_from(&msg[..]) {
                Ok(share) => match self.get_group_num_from_server().await {
                    Ok(group_n) => match shared
                        .h_apostrophe_deamons
                        .add_share(&eid, share, group_n, client_n)
                        .await
                    {
                        Ok(_) => Ok(Response::new((()))),
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    },
                    Err(_) => Err(Status::failed_precondition(
                        "cannot get group_num from server",
                    )),
                },
                Err(_) => Err(Status::invalid_argument(
                    "cannot deserialize a Share<Rational> from the decrypted bytes",
                )),
            },
            Err(e) => Err(Status::invalid_argument(format!("{}", e))),
        }
    }

    async fn select_to_share_r(&self, req: Request<Selection>) -> ResponseResult<()> {
        let Selection {
            eid,
            client_num,
            group_num,
        } = req.into_inner();
        match self.share_r(&eid, group_num as u8, client_num as u8).await {
            Ok(_) => Ok(Response::new((()))),
            Err(e) => Err(Status::internal(format!(
                "cannot share r_sequence due to {}",
                e
            ))),
        }
    }

    async fn select_to_share_h_set(&self, req: Request<Selection>) -> ResponseResult<()> {
        let Selection {
            eid,
            client_num,
            group_num,
        } = req.into_inner();
        match self
            .share_h_set(&eid, group_num as u8, client_num as u8)
            .await
        {
            Ok(_) => Ok(Response::new((()))),
            Err(e) => Err(Status::internal(format!("cannot share h_set due to {}", e))),
        }
    }

    async fn forward(&self, req: Request<RelayMessage>) -> ResponseResult<()> {
        let msg = req.into_inner();

        // check rx_uid
        let uid = self.shared.id.get_uid();
        if msg.rx_uid != uid {
            return Err(Status::invalid_argument(format!(
                "rx_uid is {}, while this is client {}",
                msg.rx_uid, uid
            )));
        }

        let eid = msg.eid;
        let client_n = msg.client_num as u8;
        let msg = msg.encrypted_message;
        match ecies::decrypt(&self.shared.sk, &msg) {
            Ok(msg) => {
                use std::convert::TryFrom;
                match Share::<Rational>::try_from(&msg[..]) {
                    Ok(msg) => match self
                        .shared
                        .forward_deamons
                        .add_received_share(&eid, msg, client_n)
                        .await
                    {
                        Ok(_) => Ok(Response::new((()))),
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    },
                    Err(_) => Err(Status::invalid_argument(
                        "cannot deserialize a Share<Rational> from the decrypted bytes",
                    )),
                }
            }
            Err(e) => Err(Status::invalid_argument(format!("{}", e))),
        }
    }
}

/// Operations about e, event confidence.
#[tonic::async_trait]
trait EventConfidenceComputation {
    /// Figure out the confidence of given event `e`, and update corresponding inner states.
    /// Implementations should make sure that the confidence of the new event is valid([0, 1]).
    async fn compute_and_add_event_confidence(
        &mut self,
        e: &Event,
        allowed_seconds_for_server: f64,
    ) -> anyhow::Result<()>;

    /// (d_{ij}, s_{ij})
    fn calculate_dsij_pair(&self, eid: &Eid, event: &Event) -> anyhow::Result<(f64, f64)>;

    /// Vec<(d_{ij}(k), s_{ij}(k))>
    fn generate_dsij_shares(
        &self,
        ds_ij_pair: (f64, f64),
        threshold: u8,
        client_n: u8,
    ) -> Vec<Share<Rational>>;

    /// await all the clients' shares and does a summation to those shares
    async fn summate_received_shares(
        &self,
        eid: &Eid,
        client_n: u8,
    ) -> anyhow::Result<Share<Rational>>;
}

/// Operations about tau, trustworthiness of user.
trait TauComputation {
    /// Compute and update the inner tau with events stored.
    /// Implementations should make sure that the new tau is valid([0, 1]).
    fn update_tau(&mut self);

    fn get_tau(&self) -> f64;
}

#[tonic::async_trait]
impl EventConfidenceComputation for AlgoClient {
    async fn compute_and_add_event_confidence(
        &mut self,
        e: &Event,
        allowed_seconds_for_server: f64,
    ) -> anyhow::Result<()> {
        if e.get_confidence() != None {
            return Ok(()); // there's no need to compute again
        }

        let ds_ij_pair = self.calculate_dsij_pair(&e.get_id(), e)?;
        let url = self.config.addrs.remote_addr.clone();
        let mut client = algo_master_client::AlgoMasterClient::connect(url.clone()).await?;

        // fetch the config of this event confidence computation session from the server
        let req = Request::new(EventConfidenceComputationConfigRequest { eid: e.get_id() });
        let process_config = client.get_event_confidence_computation_config(req).await?;
        drop(client);
        let process_config = process_config.get_ref();

        // generate shares
        let client_num = process_config.clients_pk.len() as u8;
        let shares =
            self.generate_dsij_shares(ds_ij_pair, process_config.threshold as u8, client_num);

        // dispatch shares
        let tx_uid = self.shared.id.get_uid();
        let mut relay_handles = Vec::with_capacity(shares.len());

        for share in shares {
            let message = Vec::from(&share);

            // encrypt the message
            let rx_uid = share.x as Uid;
            let rx_pk = process_config.clients_pk.get(&rx_uid);
            if let None = rx_pk {
                return Err(anyhow::anyhow!("{} does not exist in EventConfidenceComputationConfig, while share.x equals to it", rx_uid));
            }
            let rx_pk = PublicKey::from_bytes(rx_pk.unwrap())?;
            let mut rng = rand::thread_rng();
            let encrypted_message = ecies::encrypt(&rx_pk, &message[..], &mut rng)?;
            drop(rng);

            // dispatch shares
            let switch_req = RelayMessage {
                tx_uid,
                rx_uid,
                eid: e.get_id(),
                client_num: client_num as Uid,
                encrypted_message,
            };
            let switch_req = Request::new(switch_req);
            let algo_master_server_url = url.clone();
            let handle = tokio::spawn(async move {
                let mut client =
                    algo_master_client::AlgoMasterClient::connect(algo_master_server_url)
                        .await
                        .unwrap();
                client.forward(switch_req).await.unwrap();
            });
            relay_handles.push(handle);
        }

        // await **all** the forward processes
        let rts = futures::future::join_all(relay_handles).await;
        let first_dispatch_error = rts.into_iter().filter(|rt| rt.is_err()).take(1).next();
        if first_dispatch_error.is_some() {
            // return a single dispatch error
            let result = first_dispatch_error.unwrap();
            result?;
        }

        // await the (\hat{d_j^k}, \hat{s_j^k}) where k = e.get_id() from deamon(s).
        let summation_pair = self
            .summate_received_shares(&e.get_id(), client_num)
            .await?;
        let summation_pair = Vec::from(&summation_pair); // encoded into bytes

        // sumbit the summation result and get the confidence of this event
        let req = Request::new(SubmitSummationRequest {
            eid: e.get_id(),
            summation_pair,
            allowed_seconds: allowed_seconds_for_server,
        });
        let mut client = algo_master_client::AlgoMasterClient::connect(url).await?;
        let event_confidence = client.submit_summation(req).await?.into_inner();

        // update inner state: add the new event with its confidence
        let mut e = e.clone();
        e.set_confidence(event_confidence);
        self.shared.core.lock().unwrap().events.push(e);
        Ok(())
    }

    fn calculate_dsij_pair(&self, eid: &Eid, e: &Event) -> anyhow::Result<(f64, f64)> {
        let tau = self.shared.core.lock().unwrap().tau.clone();
        let d_ij = match e.get_judge() {
            Judge::True => tau,
            Judge::False => 1.0 - tau,
        };
        if *eid == e.get_id() {
            Ok((d_ij, 1.0))
        } else {
            Ok((0.0, 0.0))
        }
    }

    fn generate_dsij_shares(
        &self,
        dsij_pair: (f64, f64),
        threshold: u8,
        client_n: u8,
    ) -> Vec<Share<Rational>> {
        let dsij_pair = vec![
            BigRational::from_f64(dsij_pair.0).unwrap(),
            BigRational::from_f64(dsij_pair.1).unwrap(),
        ];
        use sharks::SecretSharingOperation;
        let mut shark = sharks::Sharks::new();
        shark
            .dealer(threshold, &dsij_pair)
            .take(client_n as usize)
            .collect()
    }

    async fn summate_received_shares(
        &self,
        eid: &Eid,
        client_n: u8,
    ) -> anyhow::Result<Share<Rational>> {
        let summation_result = self
            .shared
            .forward_deamons
            .get_summation_result(eid, client_n)
            .await?;
        Ok(summation_result)
    }
}

impl TauComputation for AlgoClient {
    fn update_tau(&mut self) {
        let mut algo_core = self.shared.core.lock().unwrap();

        let ref events = algo_core.events;
        if events.len() > 0 {
            let valid_events = events.iter().filter(|x| x.get_confidence() != None);
            let t_events = valid_events
                .clone()
                .filter(|x| x.get_judge() == Judge::True);
            let f_events = valid_events
                .clone()
                .filter(|x| x.get_judge() == Judge::False);

            let t_events_possibility_sum: f64 = t_events.map(|x| x.get_confidence().unwrap()).sum();
            let f_events_impossibility_sum: f64 =
                f_events.map(|x| 1.0 - x.get_confidence().unwrap()).sum();
            let tau = (t_events_possibility_sum + f_events_impossibility_sum)
                / valid_events.count() as f64;

            match BigRational::from_f64(tau) {
                Some(tau) => algo_core.tau = tau.to_f64().unwrap(),
                None => algo_core.tau = 0.5, // reset if counters err
            }
        } else {
            algo_core.tau = 0.5;
        }
    }

    fn get_tau(&self) -> f64 {
        let core = self.shared.core.lock().unwrap();
        core.tau.clone()
    }
}

#[tonic::async_trait]
trait TrustWorthinessAssessment {
    /// TODO: needless
    async fn get_client_n_snapshot_from_server(&self, eid: &Eid) -> anyhow::Result<Uid>;

    async fn get_group_num_from_server(&self) -> anyhow::Result<u8>;

    async fn share_tau_sequence(&self, eid: &Eid, group_n: u8, client_n: u8) -> anyhow::Result<()>;

    async fn share_r(&self, eid: &Eid, group_n: u8, client_n: u8) -> anyhow::Result<()>;

    async fn share_h_set(&self, eid: &Eid, group_n: u8, client_n: u8) -> anyhow::Result<()>;

    async fn get_trustworthiness_assessment_clients(
        &self,
        eid: &Eid,
    ) -> anyhow::Result<(HashMap<Uid, Gid>, Uid, Gid)>;

    async fn fetch_ascending_leader_board_from_server(&self, eid: &Eid)
        -> anyhow::Result<Vec<Uid>>;
}

fn make_shares(secrets: &[BigRational], t: u8, n: u8) -> anyhow::Result<Vec<Share<Rational>>> {
    let mut shark = sharks::Sharks::new();
    use sharks::SecretSharingOperation;
    Ok(shark.dealer(t, secrets).take(n as usize).collect())
}

fn generate_tau_sequence(tau: f64, group_n: usize) -> Vec<BigRational> {
    let tau = BigRational::from_f64(tau).unwrap();
    let mut res = Vec::with_capacity(group_n);
    let mut accumulator = BigRational::from_u8(1).unwrap();
    for _ in 0..group_n {
        accumulator *= tau.clone(); // TODO: do not use clone()
        res.push(accumulator.clone());
    }
    res
}

#[tonic::async_trait]
impl TrustWorthinessAssessment for AlgoClient {
    async fn get_client_n_snapshot_from_server(&self, eid: &Eid) -> anyhow::Result<Uid> {
        let url = self.config.addrs.remote_addr.clone();
        let mut client = algo_master_client::AlgoMasterClient::connect(url.clone()).await?;
        let eid = eid.clone();
        let req = Request::new(LeaderBoardComputationConfigRequest { eid });
        Ok(client
            .get_leader_board_computation_config(req)
            .await?
            .into_inner()
            .clients
            .len() as Uid)
    }

    async fn get_group_num_from_server(&self) -> anyhow::Result<u8> {
        let url = self.config.addrs.remote_addr.clone();
        let mut client = algo_master_client::AlgoMasterClient::connect(url.clone()).await?;
        let req = Request::new(());
        let group_n = client.get_group_num(req).await?.into_inner();
        Ok(group_n as u8)
    }

    async fn share_tau_sequence(&self, eid: &Eid, group_n: u8, client_n: u8) -> anyhow::Result<()> {
        // get inner current tau
        let shared = self.shared.clone();
        let tau = shared.core.lock().unwrap().tau.clone();
        let uid = shared.id.get_uid();
        drop(shared);
        let threshold = (group_n - 1) / 2 + 1; // TODO: check if `group_n` is odd

        // make shares
        let tau_sequence = generate_tau_sequence(tau, group_n as usize);
        let shares: Vec<Share<Rational>> = make_shares(&tau_sequence, threshold, group_n)?;

        // PUB shares to the server
        let mut pub_handles = Vec::with_capacity(group_n as usize);
        let url = self.config.addrs.remote_addr.clone();
        for share in shares {
            let topic_gid = share.x as i32;
            let share: Vec<u8> = Vec::from(&share);
            let eid = eid.clone();
            let notification = Some(TauSeqShareNotification {
                group_num: group_n as Gid,
                client_num: client_n as Uid,
                eid,
                uid: uid.clone(),
                share,
            });

            let req = Request::new(TauSeqSharePubRequest {
                topic_gid,
                notification,
            });
            let server_url = url.clone();

            let handle = tokio::spawn(async move {
                let mut client = algo_master_client::AlgoMasterClient::connect(server_url)
                    .await
                    .unwrap();
                client.publish_tau_sequence(req).await.unwrap();
            });
            pub_handles.push(handle);
        }
        let rts = futures::future::join_all(pub_handles).await;
        let first_dispatch_error = rts.into_iter().filter(|rt| rt.is_err()).take(1).next();
        if first_dispatch_error.is_some() {
            first_dispatch_error.unwrap()?; // return a single dispatch error
        }

        Ok(())
    }

    async fn share_r(&self, eid: &Eid, group_n: u8, client_n: u8) -> anyhow::Result<()> {
        let threshold = (group_n - 1) / 2 + 1;
        let r = vec![BigRational::from_u128(rand::random::<u128>())
            .unwrap_or(BigRational::from_u64(3).unwrap())];
        let shares = make_shares(&r, threshold, group_n)?;
        let gid = self.shared.id.get_gid();

        // PUB shares to the server
        let mut pub_handles = Vec::with_capacity(group_n as usize);
        let url = self.config.addrs.remote_addr.clone();
        for share in shares {
            let topic_gid = share.x as Gid;
            let share: Vec<u8> = Vec::from(&share);
            let eid = eid.clone();
            let notification = Some(RandCoefShareNotification {
                group_num: group_n as Gid,
                client_num: client_n as Uid,
                eid,
                share,
                gid: gid.clone(),
            });
            let req = Request::new(RandCoefSharePubRequest {
                topic_gid,
                notification,
            });
            let server_url = url.clone();
            let handle = tokio::spawn(async move {
                let mut client = algo_master_client::AlgoMasterClient::connect(server_url)
                    .await
                    .unwrap();
                client.publish_r(req).await.unwrap();
            });
            pub_handles.push(handle);
        }
        let rts = futures::future::join_all(pub_handles).await;
        let first_dispatch_error = rts.into_iter().filter(|rt| rt.is_err()).take(1).next();
        if first_dispatch_error.is_some() {
            first_dispatch_error.unwrap()?; // return a single dispatch error
        }

        Ok(())
    }

    async fn share_h_set(&self, eid: &Eid, group_n: u8, client_num: u8) -> anyhow::Result<()> {
        //! unlike described in the paper, here calculate the result only when being selected,
        //! e.i. calling this method

        let time_limitation = Duration::from_secs_f64(self.get_allowed_seconds_for_server().await);
        let h_set: Vec<BigRational> = self
            .shared
            .h_deamons
            .get_result(eid, group_n, client_num, time_limitation)
            .await?;
        let threshold = (group_n - 1) / 2 + 1;
        let shares = make_shares(&h_set, threshold, client_num as u8)?;
        drop(h_set);

        // dispatch shares
        let tx_uid = self.shared.id.get_uid();
        let url = self.config.addrs.remote_addr.clone();
        let mut relay_handles = Vec::with_capacity(shares.len());
        for share in shares {
            let switch_req = RelayMessage {
                tx_uid,
                rx_uid: share.x as Uid,
                eid: eid.clone(),
                client_num: client_num as Uid,
                encrypted_message: Vec::from(&share),
            };
            let switch_req = Request::new(switch_req);
            let server_url = url.clone();
            let handle = tokio::spawn(async move {
                let mut client = algo_master_client::AlgoMasterClient::connect(server_url)
                    .await
                    .unwrap();
                client.forward_h_share(switch_req).await.unwrap();
            });
            relay_handles.push(handle);
        }
        // await **all** the forward processes
        let rts = futures::future::join_all(relay_handles).await;
        let first_dispatch_error = rts.into_iter().filter(|rt| rt.is_err()).take(1).next();
        if first_dispatch_error.is_some() {
            first_dispatch_error.unwrap()?; // return a single dispatch error
        }

        Ok(())
    }

    async fn get_trustworthiness_assessment_clients(
        &self,
        eid: &Eid,
    ) -> anyhow::Result<(HashMap<Uid, Gid>, Uid, Gid)> {
        let server_url = self.config.addrs.remote_addr.clone();
        let mut client = algo_master_client::AlgoMasterClient::connect(server_url).await?;
        let leader_board_computation_config = client
            .get_leader_board_computation_config(Request::new(
                LeaderBoardComputationConfigRequest { eid: eid.clone() },
            ))
            .await?
            .into_inner();

        let clients = leader_board_computation_config.clients;
        let client_n = clients.len() as Uid;
        let groups: HashSet<Gid> = clients.iter().map(|client| client.1).cloned().collect();
        let group_n = groups.len() as Gid;
        Ok((clients, client_n, group_n))
    }

    async fn fetch_ascending_leader_board_from_server(
        &self,
        eid: &Eid,
    ) -> anyhow::Result<Vec<Uid>> {
        // fetch the configuration for this round of trustworthiness assessment
        let (_clients, client_n, group_n) =
            self.get_trustworthiness_assessment_clients(eid).await?;

        // generate & publish tau_sequence
        self.share_tau_sequence(eid, group_n as u8, client_n as u8)
            .await?;

        // await the h'(uid) for this round
        let h_apostrophe_deamons_threshold = ((group_n - 1) / 2 + 1) as u8;
        let allowed_seconds = self.get_allowed_seconds_for_server().await;
        let time_limitation = Duration::from_secs_f64(allowed_seconds);
        let h_apostrophe_share = self
            .shared
            .h_apostrophe_deamons
            .get_result(
                eid,
                h_apostrophe_deamons_threshold,
                client_n as u8,
                time_limitation,
            )
            .await?;

        let server_url = self.config.addrs.remote_addr.clone();
        let mut algo_client = algo_master_client::AlgoMasterClient::connect(server_url).await?;
        let req = Request::new(HApoShare {
            eid: eid.clone(),
            allowed_seconds,
            share: Vec::from(&h_apostrophe_share),
        });
        let leader_board = algo_client.submit_h_apo_share(req).await?.into_inner();
        Ok(leader_board.clients)
    }
}

#[tonic::async_trait]
trait AlgoClientUtil: EventConfidenceComputation + TauComputation + TrustWorthinessAssessment {
    async fn get_allowed_seconds_for_server(&self) -> f64;

    async fn register(
        server_url: String,
        mailbox_addr: String,
        pk: PublicKey,
    ) -> anyhow::Result<Id>;

    async fn register_event(&self, event_identifier: String) -> anyhow::Result<Eid>;

    async fn new_algo_client(config: ClientConfig) -> anyhow::Result<AlgoClient>;

    fn generate_keypair() -> (SecretKey, PublicKey);

    async fn listen_to(&mut self, mut event_src: EventIdentifierReceiver, error_rate: f64);
}

#[tonic::async_trait]
impl AlgoClientUtil for AlgoClient {
    #[inline]
    async fn get_allowed_seconds_for_server(&self) -> f64 {
        // TODO: get data from self.config
        const ALLOWED_SECONDS: f64 = (5 * 60 * 60) as f64;
        ALLOWED_SECONDS
    }

    async fn register(
        server_url: String,
        mailbox_addr: String,
        pk: PublicKey,
    ) -> anyhow::Result<Id> {
        let pk = pk.to_bytes().to_vec();
        let mut client = algo_master_client::AlgoMasterClient::connect(server_url).await?;
        let req = Request::new(InitRequest {
            pk,
            mailbox: mailbox_addr,
        });
        let InitResponse { uid, gid } = client.register(req).await?.into_inner();
        Ok(Id::new(uid, gid))
    }

    async fn register_event(&self, event_identifier: String) -> anyhow::Result<Eid> {
        let server_url = self.config.addrs.remote_addr.clone();
        let mut client = algo_master_client::AlgoMasterClient::connect(server_url).await?;
        let EventRegistrationResponse { eid } = client
            .find_or_register_event(Request::new(EventRegistrationRequest {
                identifier: event_identifier,
            }))
            .await?
            .into_inner();
        Ok(eid)
    }

    async fn new_algo_client(config: ClientConfig) -> anyhow::Result<Self> {
        let (sk, pk) = Self::generate_keypair();
        let core = Mutex::new(AlgoCore::new());

        // register to get id(uid, gid)
        let server_url = config.addrs.remote_addr.clone();
        let mailbox_addr = config.addrs.mailbox_addr.clone();
        let id = Self::register(server_url, mailbox_addr, pk).await?;

        // construct a relay processor
        let forward_deamons = ForwardDeamonSet::new();
        let h_apostrophe_deamons = DeamonSet::new();
        let h_deamons = DeamonSet::new();

        let shared = Shared {
            id,
            core,
            sk,
            forward_deamons,
            h_apostrophe_deamons,
            h_deamons,
        };
        let shared = Arc::new(shared);
        Ok(AlgoClient { shared, config })
    }

    async fn listen_to(&mut self, mut event_src: EventIdentifierReceiver, error_rate: f64) {
        assert!(
            error_rate <= 1.0 && error_rate >= 0.0,
            "error_rate out of range"
        );
        let weights = vec![error_rate, 1.0 - error_rate];
        let distribution =
            rand::distributions::weighted::alias_method::WeightedIndex::new(weights).unwrap();

        loop {
            let event_identifier = event_src.recv().await;
            if let Err(_e) = event_identifier {
                // event_src is dried
                break;
            }
            let event_identifier = event_identifier.unwrap();
            // tokio::spawn(async move {
            //     TODO: async thread => may cause some sync errors
            // }).await;
            async {
                let eid = self.register_event(event_identifier).await.unwrap();
                use rand::distributions::Distribution;
                let judge = distribution.sample(&mut rand::thread_rng());
                let event = Event::new(eid, judge);

                self.compute_and_add_event_confidence(
                    &event,
                    self.get_allowed_seconds_for_server().await,
                )
                .await
                .unwrap();
                self.update_tau();
                let leader_board = self
                    .fetch_ascending_leader_board_from_server(&event.get_id())
                    .await
                    .unwrap();
                println!(
                    "[Eid: {}] Leader Board (Ascending): {:?}",
                    event.get_id(),
                    leader_board
                );
            }
            .await; // TODO: handle the result
        }
    }

    fn generate_keypair() -> (SecretKey, PublicKey) {
        let mut rng = rand::thread_rng();
        ecies::generate_keypair(&mut rng)
    }
}
