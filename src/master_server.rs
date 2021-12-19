tonic::include_proto!("master");
tonic::include_proto!("slave");
use super::{
    client_management::{ClientManagement, SlaveInfo},
    event::{Eid, EidAssigner, Event, EventIdentifier, Judge},
    algo_util::{generate_dsij_shares, make_shares},
    id::{Gid, Uid, UidAssigner},
    ResponseResult,
};

use anyhow::Result;
use rand::{prelude::Distribution, Rng};
use sharks::{secret_type::Rational, Share, Sharks};
use slog::{crit, debug, error, info, trace, warn, Logger};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::collections::{HashMap, HashSet};
use tokio::sync::{Barrier, Notify, RwLock};
use tonic::{Request, Response, Status};
use ecies_ed25519::{self as ecies, PublicKey};

/// Shared fields among requests
#[derive(Debug)]
struct Shared {
    logger: Logger,
    group_n: Gid,  // the number of groups
    client_n: Uid, // the number of clients
    error_rate: f64,

    uid_assigner: UidAssigner,
    pub clients: RwLock<HashMap<Uid, Arc<SlaveInfo>>>,
    registration_barrier: Barrier,
    registation_return_barrier: Barrier, 

    iteration_notifier: Notify,

    /// stores the ascending leader board of clients' trustworthiness.
    leader_board: RwLock<Vec<Uid>>,
}

/// A master server instance
#[derive(Debug)]
pub struct MasterServer {
    shared: Arc<Shared>,
}

impl MasterServer {
    pub fn new<L: Into<Logger>>(group_n: Gid, client_n: Uid, error_rate: f64, logger: L) -> Self {
        let logger = logger.into();
        let shared = Arc::new(Shared {
            logger: logger.clone(),
            group_n,
            client_n,
            error_rate,
            uid_assigner: UidAssigner::default(),
            clients: RwLock::new(HashMap::new()),
            registration_barrier: Barrier::new(client_n as usize),
            registation_return_barrier: Barrier::new(client_n as usize),
            iteration_notifier: Notify::new(),

            leader_board: RwLock::new(Vec::new()),
        });

        MasterServer { shared }
    }

    fn get_init_client_n(&self) -> usize {
        self.shared.client_n as usize
    }
}

type ConfidenceDict = HashMap<Eid, Vec<f64>>;

#[tonic::async_trait]
pub trait Iteration {
    /// returns: { eid: [confidence array] }
    async fn compute_event_confidence(&self) -> Result<HashMap<Eid, Result<Vec<f64>>>>;

    async fn update_trustworthiness(&self, confidences: HashMap<Eid, Vec<f64>>) -> Result<()>;

    async fn iterate(&self) -> Result<ConfidenceDict, (anyhow::Error, ConfidenceDict)>;

    async fn wrapped_iterate(&self);

    fn iteration_condition(round_num: usize, average_dist: f64) -> bool;

    async fn drop_off(&self);
}
#[tonic::async_trait]
impl Iteration for Arc<MasterServer> {
    async fn compute_event_confidence(&self) -> Result<HashMap<Eid, Result<Vec<f64>>>> {
        let clients = self.get_all_clients().await;
        // {tx_uid: {eid: {rx_uid: bytes}}}
        let mut tx_pairs_shares = HashMap::with_capacity(clients.len());
        for client in clients.iter() {
            let server_url = format!("http://{}", client.mailbox);
            let mut iteration_chan = iteration_client::IterationClient::connect(server_url).await?;
            let dsij_pairs_shares = iteration_chan
                .get_dsij_pairs_shares(())
                .await?
                .into_inner()
                .value;
            let dsij_pairs_shares: HashMap<Eid, _> = dsij_pairs_shares
                .into_iter()
                .map(|(eid, dsij_pairs_for_one_event)| (eid, dsij_pairs_for_one_event.value))
                .collect();
            tx_pairs_shares.insert(client.uid.clone(), dsij_pairs_shares);
        }
        // Event Complement
        // let mut all_eids = HashSet::new();
        // clients.iter().map(|c| c.identifier_map.values()).for_each(|eids| {
        //     all_eids.extend(eids);
        // });
        // for (tx_uid, shares_of_events) in &mut tx_pairs_shares {
        //     let mentioned_eids = shares_of_events.keys().cloned().collect();
        //     let unmentioned_eids = all_eids.difference(&mentioned_eids);
        //     for eid in unmentioned_eids {
        //         let dsij = (0.0f64, 0.0f64);
        //         let client_n = clients.len();
        //         let mut encrypted_shares_of_one_event = HashMap::new();
        //         let shares = generate_dsij_shares(dsij, client_n / 2, client_n);
        //         for share in shares {
        //             let rx_uid = share.x as Uid;
        //             let share = Vec::from(&share);
        //             let pk = PublicKey::from_bytes(&self.get_client_by_uid(&tx_uid).await.unwrap().pk).unwrap();
        //             let encrypted_share =
        //                 ecies::encrypt(&pk, &share[..], &mut rand::thread_rng()).unwrap();
        //             encrypted_shares_of_one_event.insert(rx_uid, encrypted_share);
        //         }
        //         shares_of_events.insert(eid.clone(), encrypted_shares_of_one_event);// {rx_uid: encrypted_zero_pair}
        //     }
        // }

        // transform shares:
        // {tx_uid: {eid: {rx_uid: encrypted_pair}}}
        // =>
        // {rx_uid: {eid: [encrypted_pair from distinct tx_clients]}}
        let mut rx_pairs_shares: HashMap<Uid, _> = HashMap::with_capacity(clients.len());
        for (_tx_uid, shares_of_events) in tx_pairs_shares {
            for (eid, shares_of_one_event) in shares_of_events {
                for (rx_uid, share) in shares_of_one_event {
                    if !rx_pairs_shares.contains_key(&rx_uid) {
                        rx_pairs_shares
                            .insert(rx_uid.clone(), HashMap::with_capacity(clients.len()));
                    }
                    let pairs = rx_pairs_shares.get_mut(&rx_uid).unwrap();
                    if !pairs.contains_key(&eid) {
                        pairs.insert(eid.clone(), Vec::new());
                    }
                    pairs.get_mut(&eid).unwrap().push(share);
                }
            }
        }

        // await at least threshold responses TODO: what if can not receive threshold responses at last
        let threshold = self.get_init_client_n() / 2;
        let mut dsij_sum_pairs_set = Vec::with_capacity(threshold); // [{ eid: sum_pair }]
        for (rx_uid, shares_of_events) in rx_pairs_shares {
            let client = self.get_client_by_uid(&rx_uid).await.unwrap();
            if !*client.online.read().unwrap() {
                continue;
            }
            let server_url = format!("http://{}", client.mailbox);
            let mut iter_chan = iteration_client::IterationClient::connect(server_url).await?;
            let dsij_sum_pairs = iter_chan
                .get_dsij_sum_pairs(DsijSumPairsReq {
                    dsij_pairs: shares_of_events
                        .into_iter()
                        .map(|(eid, enc_pairs)| (eid, BytesVec { value: enc_pairs }))
                        .collect(),
                })
                .await?
                .into_inner()
                .value;
            dsij_sum_pairs_set.push(dsij_sum_pairs);
            // if dsij_sum_pairs_set.len() >= threshold {
            //     break;
            // }
        }
        if dsij_sum_pairs_set.len() < threshold {
            return Err(anyhow::anyhow!("The number of summation pairs are {}, less than {}", dsij_sum_pairs_set.len(), threshold));
        }

        // resolve (d_j, s_j) and return the events' confidences
        // { eid: [sum_pair] }
        let mut dsij_sum_pairs_for_each_event = HashMap::new();
        for set in dsij_sum_pairs_set {
            for (eid, sum_pair) in set {
                if !dsij_sum_pairs_for_each_event.contains_key(&eid) {
                    dsij_sum_pairs_for_each_event.insert(eid.clone(), Vec::new());
                }
                // deserialize the sum_pair
                let sum_pair = Share::<Rational>::try_from(&sum_pair[..]).unwrap();
                dsij_sum_pairs_for_each_event
                    .get_mut(&eid)
                    .unwrap()
                    .push(sum_pair);
            }
        }

        let confidences = dsij_sum_pairs_for_each_event
            .into_iter()
            .map(|(eid, sum_pairs)| {
                let mut shark = sharks::Sharks::new();
                shark.threshold = threshold as u8;
                use sharks::SecretSharingOperation;
                let confidence_result = match shark.recover(sum_pairs) {
                    Ok(secret_res) => {
                        let sum_d = secret_res[0].clone();
                        let sum_s = secret_res[1].clone();
                        let rho_j = sum_d / sum_s;

                        use num_traits::ToPrimitive;
                        match rho_j.to_f64() {
                            Some(rho_j) => Ok(vec![rho_j, 1.0 - rho_j]),
                            None => Err(anyhow::anyhow!("Invalid conversion of rho_j")),
                        }
                    }
                    Err(_) => Err(anyhow::anyhow!("Shares not enough for recover rho_j")),
                };
                (eid, confidence_result)
            })
            .collect();
        Ok(confidences)
    }

    async fn update_trustworthiness(&self, confidences: HashMap<Eid, Vec<f64>>) -> Result<()> {
        let clients = self.get_online_clients().await;
        for client in clients {
            let owned_confidences = client
                .identifier_map
                .values()
                .map(|eid| {
                    (
                        eid.clone(),
                        Confidence {
                            value: match confidences.get(eid) {
                                Some(conf) => conf.clone(),
                                None => vec![0.5, 0.5]
                            },
                        },
                    )
                })
                .collect();
            let owned_confidences = Confidences {
                value: owned_confidences,
            };

            let server_url = format!("http://{}", client.mailbox);
            let mut iter_chan = iteration_client::IterationClient::connect(server_url).await?;
            iter_chan
                .update_trustworthiness_with(owned_confidences)
                .await?;
        }

        Ok(())
    }

    async fn iterate(&self) -> Result<ConfidenceDict, (anyhow::Error, ConfidenceDict)> {
        let mut round_num = 0;
        let mut average_dist: f64 = 1.0;
        let mut confidences_temp: Option<HashMap<Eid, Vec<f64>>> = None;
        while Self::iteration_condition(round_num, average_dist) {
            let confidences = self.compute_event_confidence().await;
            if let Err(e) = confidences {
                return Err((e, confidences_temp.unwrap_or_default()));
            }
            let confidences = confidences.unwrap();

            let solved_confidences: HashMap<Eid, Vec<f64>> = confidences
                .iter()
                .filter(|(_eid, conf)| conf.is_ok())
                .map(|(eid, conf)| (eid.clone(), conf.as_ref().unwrap().clone()))
                .collect();
            let unsolved_eids: Vec<Eid> = confidences.into_iter()
                .filter(|(_eid, conf)| conf.is_err())
                .map(|(eid, _conf)| eid).collect();
            info!(self.shared.logger, "Failed events' eids"; "round_ith" => round_num, "the number of failed eids" => unsolved_eids.len());

            let updated_confidences = if confidences_temp.is_none() {
                solved_confidences
            } else {
                let mut temp = confidences_temp.unwrap();
                average_dist = 0.0;
                let n = solved_confidences.len() as f64;
                for (eid, confidence) in solved_confidences {
                    // update the events' confidences
                    let old_confidence = temp.get_mut(&eid).unwrap();
                    average_dist += (confidence[0] - old_confidence[0]).abs();
                    *old_confidence = confidence;
                }
                average_dist /= n;
                temp
            };
            confidences_temp = Some(updated_confidences.clone());
            if let Err(e) = self.update_trustworthiness(updated_confidences).await {
                return Err((e, confidences_temp.unwrap_or_default()));
            }

            round_num += 1;
            info!(self.shared.logger, "Iteration"; "round_ith" => round_num);
            self.drop_off().await;
        }

        Ok(confidences_temp.unwrap_or_default())
    }

    async fn wrapped_iterate(&self) {
        self.shared.iteration_notifier.notified().await;
        tokio::time::sleep(Duration::from_secs(5)).await; // reserved for clients to be ready
        info!(self.shared.logger, "Iterations start");

        let confidences_to_string = |confidences: ConfidenceDict| -> String {
            let confidences: Vec<_> = confidences.iter().map(|(eid, conf)| {
                format!("({}: {}) ", eid.clone(), conf[0])
            }).collect();
            let mut confidence_string = String::new();
            confidences.into_iter().for_each(|conf| confidence_string.push_str(&conf));
            confidence_string
        };

        match self.iterate().await {
            Err((e, confidences)) => {
                let confidences = confidences_to_string(confidences);
                error!(self.shared.logger, "Iterations end"; "event_confidence" => confidences, "error" => %e);
            }
            Ok(confidences) => {
                let confidences = confidences_to_string(confidences);
                info!(self.shared.logger, "Iterations end"; "event_confidence" => confidences);
            }
        }
    }

    async fn drop_off(&self) {
        let error_rate = self.shared.error_rate;
        let weights: Vec<f64> = vec![1.0 - error_rate, error_rate];
        let distribution =
            rand::distributions::weighted::alias_method::WeightedIndex::new(weights).unwrap();
        let online_choices = [true, false];

        let mut clients_w = self.shared.clients.write().await;
        let mut rng = rand::thread_rng();
        for (_uid, client) in clients_w.iter_mut() {
            let mut online_w = client.online.write().unwrap();
            if !*online_w {
                continue;
            }
            *online_w = online_choices[distribution.sample(&mut rng)];
        }
    }

    fn iteration_condition(round_num: usize, average_dist: f64) -> bool {
        round_num <= 10 // iterate for 10 times
        && average_dist > 0.01
    }
}

#[tonic::async_trait]
pub trait Rank {
    async fn rank_clients_trustworthiness(&self) -> Result<Vec<Uid>>;

    fn get_gid(&self, uid: &Uid) -> Gid;

    fn generate_lambda_sequence(group_n: usize) -> Vec<f64> {
        use nalgebra::DMatrix;
        use num_traits::pow;
        let dm = DMatrix::from_fn(group_n, group_n, |i, j| pow(i as f64 + 1.0, j));
        let dm = dm.try_inverse().unwrap();
        let dm = dm.row(0);
        dm.column_iter().map(|col| col[(0, 0)]).collect()
    }
}
#[tonic::async_trait]
impl Rank for Arc<MasterServer> {
    #[inline]
    fn get_gid(&self, uid: &Uid) -> Gid {
        let gid = uid.clone() % self.shared.group_n;
        if gid == 0 {
            self.shared.group_n
        } else {
            gid
        }
    }

    async fn rank_clients_trustworthiness(&self) -> Result<Vec<Uid>> {
        // group all the online clients
        let mut groups: HashMap<Gid, Vec<Uid>> =
            (1..=self.shared.group_n).map(|gid| (gid, vec![])).collect();
        let clients = self.get_online_clients().await;
        clients.iter().for_each(|client| {
            let gid = self.get_gid(&client.uid);
            groups.get_mut(&gid).unwrap().push(client.uid.clone());
        });

        // fetch tau_sequence & random_coefficients
        let mut uid_gid_map = HashMap::new();
        groups.iter().for_each(|(gid, uids)| {
            uids.iter().for_each(|uid| {
                uid_gid_map.insert(uid.clone(), gid.clone());
            });
        });
        let group_info = GroupInfo { uid_gid_map };

        // fetch tau_sequences
        let mut tau_seq_shares: HashMap<Uid, HashMap<Uid, _>> =
            HashMap::with_capacity(clients.len());
        for client in &clients {
            let server_url = format!("http://{}", client.mailbox);
            let mut rank_chan = rank_client::RankClient::connect(server_url).await?;

            // { rx_uid: encrpyted_share(Share<Rational>) }
            let tau_sequence_shares = rank_chan
                .get_tau_seq_shares(group_info.clone())
                .await?
                .into_inner()
                .value;
            let tx_uid = client.uid.clone();
            tau_seq_shares.insert(tx_uid, tau_sequence_shares);
        } // { tx_uid: { rx_uid: encrpyted_share(Share<Rational>) } }
        let mut rx_tau_seq_shares = HashMap::new();
        tau_seq_shares.into_iter().for_each(|(tx_uid, shares)| {
            shares.into_iter().for_each(|(rx_uid, share)| {
                if !rx_tau_seq_shares.contains_key(&rx_uid) {
                    rx_tau_seq_shares.insert(rx_uid.clone(), HashMap::new());
                }
                rx_tau_seq_shares
                    .get_mut(&rx_uid)
                    .unwrap()
                    .insert(tx_uid.clone(), share);
            });
        });
        // { rx_uid: { tx_uid: encrpyted_share(Share<Rational>) } }
        let tau_seq_shares = rx_tau_seq_shares;

        // fetch r_shares from the chosen ones
        let mut r_shares: HashMap<Gid, HashMap<Uid, _>> = HashMap::with_capacity(groups.len());
        for (gid, uids) in &groups {
            // choose one uid from uids randomly
            let index: usize = rand::thread_rng().gen_range(0, uids.len());
            let the_chosen_one = uids[index];
            let the_chosen_one = self.get_client_by_uid(&the_chosen_one).await.unwrap();

            let server_url = format!("http://{}", the_chosen_one.mailbox);
            let mut rank_chan = rank_client::RankClient::connect(server_url).await?;
            // { rx_uid: encrpyted_share(Share<Rational>) }
            let r_shares_of_one = rank_chan
                .get_r_shares(group_info.clone())
                .await?
                .into_inner()
                .value;
            r_shares.insert(gid.clone(), r_shares_of_one);
        } // { tx_gid: { rx_uid: encrpyted_share(Share<Rational>) } }
        let mut rx_r_shares = HashMap::new();
        r_shares.into_iter().for_each(|(tx_gid, shares_of_one_group)| {
            shares_of_one_group.into_iter().for_each(|(rx_uid, share)| {
                if !rx_r_shares.contains_key(&rx_uid) {
                    rx_r_shares.insert(rx_uid.clone(), HashMap::new());
                }
                rx_r_shares.get_mut(&rx_uid).unwrap().insert(tx_gid.clone(), share);
            });
        });
        // { rx_uid: { tx_gid: encrpyted_share(Share<Rational>) } }
        let r_shares = rx_r_shares;

        // choose and fetch h_set
        let lambda_seq = Self::generate_lambda_sequence(self.shared.group_n as usize);
        let mut h_set_shares_list = HashMap::new();
        for (gid, uids) in &groups {
            // choose one uid from uids randomly
            let index: usize = rand::thread_rng().gen_range(0, uids.len());
            let the_chosen_one = uids[index];
            let the_chosen_one = self.get_client_by_uid(&the_chosen_one).await.unwrap();

            let server_url = format!("http://{}", the_chosen_one.mailbox);
            let mut rank_chan = rank_client::RankClient::connect(server_url).await.unwrap();

            // { rx_uid: encrpyted_share(Share<Rational>) }
            let tau_seq_shares = tau_seq_shares.get(&the_chosen_one.uid).unwrap().clone();
            let r_shares = r_shares.get(&the_chosen_one.uid).unwrap().clone();
            let lambda = lambda_seq[gid.clone() as usize - 1].clone();
            let req = HSetSharesReq {
                tau_seq_shares, // { tx_uid: encrpyted_share(Share<Rational>) }
                r_shares,       // { tx_gid: encrpyted_share(Share<Rational>) }
                lambda,
            };
            let h_set_shares = rank_chan.get_h_set_shares(req).await?.into_inner().value;
            h_set_shares_list.insert(gid.clone(), h_set_shares);
        } // { tx_gid: { rx_uid: encrpyted_share(Share<Rational>) } }
        let mut rx_h_set_shares_list = HashMap::new();
        h_set_shares_list
            .into_iter()
            .for_each(|(tx_gid, h_set_shares)| {
                h_set_shares.into_iter().for_each(|(rx_uid, share)| {
                    if !rx_h_set_shares_list.contains_key(&rx_uid) {
                        rx_h_set_shares_list.insert(rx_uid.clone(), HashMap::new());
                    }
                    rx_h_set_shares_list
                        .get_mut(&rx_uid)
                        .unwrap()
                        .insert(tx_gid.clone(), share);
                });
            });
        // { rx_uid: { tx_gid: encrpyted_share(Share<Rational>) } }
        let h_set_share_list = rx_h_set_shares_list;

        // fetch h_apo_set
        let mut h_apo_set_shares: HashMap<Uid, _> = HashMap::new();
        for client in &clients {
            let server_url = format!("http://{}", client.mailbox);
            let mut rank_chan = rank_client::RankClient::connect(server_url).await?;
            let req = HApoSetReq {
                value: h_set_share_list.get(&client.uid).unwrap().clone(),
            };
            let h_apo_set_share = rank_chan.get_h_apo_set(req).await?.into_inner().value;
            h_apo_set_shares.insert(client.uid.clone(), h_apo_set_share);
        }
        // recovery h_i
        let mut uids: Vec<Uid> = clients.iter().map(|client| client.uid.clone()).collect();
        uids.sort_unstable(); // ascendingly
        let shares: Vec<Share<Rational>> = h_apo_set_shares
            .values()
            .map(|share| Share::<Rational>::try_from(&share[..]).unwrap())
            .collect();
        let mut shark = Sharks::new();
        shark.threshold = (groups.len() as u8 - 1) / 2 + 1;
        use sharks::SecretSharingOperation;
        match shark.recover(shares) {
            Ok(h_set) => {
                let mut temp: Vec<_> = h_set.iter().zip(uids.into_iter()).collect();
                temp.sort_unstable_by_key(|pair| pair.0);
                Ok(temp.into_iter().map(|pair| pair.1).collect())
            }
            Err(_) => Err(anyhow::anyhow!("Share not enough")),
        }
    }
}

#[tonic::async_trait]
impl ClientManagement for Arc<MasterServer> {
    fn new_uid(&self) -> Uid {
        self.shared.uid_assigner.new_uid()
    }

    fn get_group_n(&self) -> Gid {
        self.shared.group_n
    }

    async fn add_client(
        &self,
        pk: &[u8],
        mailbox: &SocketAddr,
        identifiers: Vec<String>,
    ) -> Option<Arc<SlaveInfo>> {
        match self.get_client_by_pk(pk).await {
            None => {
                let uid = self.new_uid();
                let pk = pk.to_vec();
                let mut identifier_map = HashMap::with_capacity(identifiers.len());
                identifiers.iter().for_each(|identifier| {
                    identifier_map.insert(identifier.clone(), Eid::default());
                });
                let new_client = Arc::new(SlaveInfo {
                    uid,
                    pk,
                    mailbox: mailbox.clone(),
                    identifier_map,
                    online: Arc::new(std::sync::RwLock::new(true)),
                });

                let mut clients_w = self.shared.clients.write().await;
                clients_w.insert(new_client.uid, new_client.clone());

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

    async fn get_all_clients(&self) -> Vec<Arc<SlaveInfo>> {
        let clients_r = self.shared.clients.read().await;
        clients_r.values().cloned().collect()
    }

    async fn get_online_clients(&self) -> Vec<Arc<SlaveInfo>> {
        let clients_r = self.shared.clients.read().await;
        clients_r
            .values()
            .filter(|client| *client.online.read().unwrap())
            .cloned()
            .collect()
    }
}

#[tonic::async_trait]
impl registration_server::Registration for Arc<MasterServer> {
    async fn register(&self, req: Request<RegistrationReq>) -> ResponseResult<RegistrationRes> {
        let RegistrationReq {
            pk,
            mailbox,
            identifiers,
        } = req.into_inner();
        // register this client and events
        match mailbox.parse() {
            Ok(mailbox) => match self.add_client(&pk, &mailbox, identifiers).await {
                Some(new_one) => {
                    info!(self.shared.logger, "registration";
                        "new client" => %new_one,
                    );
                }
                None => {
                    return Err(Status::already_exists("Public key has been registered"));
                }
            },
            Err(e) => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid mailbox addr: {}",
                    e
                )));
            }
        }

        // await all the clients until they finished their addings
        let barrier_result = self.shared.registration_barrier.wait().await;

        // the leader assigns ids for clients and events
        if barrier_result.is_leader() {
            let clients = self.get_all_clients().await;
            let mut event_set = HashMap::new();
            let eid_assigner = EidAssigner::default();
            for client in clients {
                for identifier in client.identifier_map.keys() {
                    if !event_set.contains_key(identifier) {
                        event_set.insert(identifier.clone(), eid_assigner.new_id());
                    }
                }
            }
            let event_set = event_set;

            // log identifier_eid_map
            let identifier_eid_pairs: Vec<_> = event_set.iter().map(|(identifier, eid)| {
                format!("({}: {}) ", identifier, eid)
            }).collect();
            let mut identifier_eid_str = String::new();
            identifier_eid_pairs.into_iter().for_each(|pair| identifier_eid_str.push_str(&pair));
            info!(self.shared.logger, "Identifier Eid Map"; "pairs(event identifier, eid)" => identifier_eid_str);

            // modify inner status
            let mut clients_w = self.shared.clients.write().await;
            clients_w.iter_mut().for_each(|(_uid, client)| {
                let mut tmp = client.as_ref().clone();
                tmp.identifier_map.iter_mut().for_each(|(identifier, eid)| {
                    *eid = event_set.get(identifier).unwrap().clone();
                });
                *client = Arc::new(tmp);
            });
        }
        // block other requests until data integration ends
        let barrier_result = self.shared.registation_return_barrier.wait().await;

        // return uid and eids
        match self.get_client_by_pk(&pk).await {
            None => Err(Status::data_loss("Registered client got lost")),
            Some(client) => {
                if barrier_result.is_leader() {
                    self.shared.iteration_notifier.notify_one();
                }
                Ok(Response::new(RegistrationRes {
                    uid: client.uid,
                    identifier_eid_map: client.identifier_map.clone(),
                }))
            }
        }
    }
}

#[tonic::async_trait]
impl security_server::Security for Arc<MasterServer> {
    async fn get_public_keys(&self, req: Request<PublicKeyQuery>) -> ResponseResult<ClientsPk> {
        let PublicKeyQuery { online } = req.into_inner();
        let clients = if online {
            self.get_online_clients().await
        } else {
            self.get_all_clients().await
        };
        let mut public_keys = HashMap::with_capacity(clients.len());
        clients.into_iter().for_each(|client| {
            public_keys.insert(client.uid.clone(), client.pk.clone());
        });
        Ok(Response::new(ClientsPk { value: public_keys }))
    }
}
