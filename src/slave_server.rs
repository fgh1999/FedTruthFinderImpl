tonic::include_proto!("slave");
tonic::include_proto!("master");
use crate::event::RawEventRecord;

use super::{
    algo_util::{generate_dsij_shares, make_shares},
    config::ClientConfig,
    event::{Eid, Event, Judge},
    fmt_leader_board,
    id::{Gid, Uid},
    ResponseResult,
};
use rand::distributions;
use tonic::{Request, Response, Status};

use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
};
use tokio::sync::RwLock;

use num_rational::BigRational;
use num_traits::cast::{FromPrimitive, ToPrimitive};

use ecies_ed25519::{self as ecies, PublicKey, SecretKey};
// use rand::distributions::weighted::alias_method::{WeightedIndex, Weight};
use sharks::{secret_type::Rational, Share};
use slog::{crit, debug, error, info, trace, warn, Logger};

#[derive(Debug)]
pub struct SlaveServer {
    config: ClientConfig,
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    uid: Uid,
    sk: SecretKey,
    core: RwLock<AlgoCore>,
    logger: Logger,
}

#[derive(Debug)]
struct AlgoCore {
    pub events: HashMap<Eid, Event>,
    pub tau: f64,
}

#[allow(dead_code)]
impl AlgoCore {
    /// Construct a new AlgoCore with a random tau within [0.8, 1.0]
    pub fn new() -> Self {
        AlgoCore {
            events: HashMap::new(),
            tau: Self::generate_tau(),
        }
    }

    /// Construct a new AlgoCore with a given tau
    pub fn with_tau(tau: f64) -> Self {
        AlgoCore {
            events: HashMap::new(),
            tau,
        }
    }

    pub fn reset_tau(&mut self) {
        self.tau = Self::generate_tau();
    }

    pub fn generate_tau() -> f64 {
        use rand::prelude::*;
        let range = 0.2;
        let mut rng = rand::thread_rng();
        let disturbance: f64 = (rng.gen::<f64>() - 0.5) * range;
        0.9 + disturbance
        // 0.9 // for consistency in test
    }
}

#[tonic::async_trait]
impl iteration_server::Iteration for Arc<SlaveServer> {
    async fn get_dsij_pairs_shares(&self, _req: Request<()>) -> ResponseResult<DsijPairsForEvents> {
        loop {
            let core_r = self.shared.core.read().await;
            if core_r.events.len() > 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }

        // figure out events' encrypted pairs
        let core_r = self.shared.core.read().await;
        let tau = core_r.tau.clone();

        // fetch all the clients' public keys
        let server_url = self.config.addrs.remote_addr.clone();
        let mut security_chan = security_client::SecurityClient::connect(server_url)
            .await
            .unwrap();
        let pks = security_chan
            .get_public_keys(PublicKeyQuery { online: false })
            .await?
            .into_inner()
            .value;
        let pks: HashMap<_, _> = pks
            .into_iter()
            .map(|(uid, pk_bytes)| (uid, PublicKey::from_bytes(&pk_bytes).unwrap()))
            .collect();

        let dsij_pairs_shares = core_r
            .events
            .values()
            .map(|event| {
                let dsij = (
                    match event.get_judge() {
                        Judge::True => tau,
                        Judge::False => 1.0 - tau,
                    },
                    1.0,
                );
                let client_n = pks.len();
                let shares = generate_dsij_shares(dsij, client_n / 2, client_n);
                let pairs_for_one_event = shares
                    .into_iter()
                    .map(|share| {
                        let rx_uid = share.x as Uid;
                        let share = Vec::from(&share);
                        let pk = pks.get(&rx_uid);
                        if pk.is_none() {
                            error!(self.shared.logger, "Can not find this uid in pks"; "uid" => rx_uid);
                            panic!("");
                        }
                        let pk = pk.unwrap();
                        let encrypted_share =
                            ecies::encrypt(pk, &share[..], &mut rand::thread_rng()).unwrap();
                        (rx_uid, encrypted_share)
                    })
                    .collect();
                let pairs_for_one_event = DsijPairsForOneEvent {
                    value: pairs_for_one_event,
                };
                (event.get_id(), pairs_for_one_event)
            })
            .collect();
        info!(self.shared.logger, "get_dsij_pairs_shares"; "the number of public keys" => pks.len());

        Ok(Response::new(DsijPairsForEvents {
            value: dsij_pairs_shares,
        }))
    }

    async fn get_dsij_sum_pairs(
        &self,
        req: Request<DsijSumPairsReq>,
    ) -> ResponseResult<DsijSumPairs> {
        let eid_pairs_map: HashMap<_, _> = req
            .into_inner()
            .dsij_pairs
            .into_iter()
            .map(|(eid, bytes_vec)| {
                let pairs = bytes_vec.value;
                let pairs: Vec<_> = pairs
                    .into_iter()
                    .map(|pair| {
                        Share::<Rational>::try_from(
                            &ecies::decrypt(&self.shared.sk, &pair).unwrap()[..],
                        )
                        .unwrap()
                    })
                    .collect();
                (eid, pairs)
            })
            .collect(); // { eid: [Share<Rational>(.y == [D_ij(k), S_ij(k)])] }

        let sum_pairs: HashMap<Eid, Share<Rational>> = eid_pairs_map
            .into_iter()
            .map(|(eid, shares)| {
                let x = shares[0].x.clone();
                let d_jk: Rational = shares.iter().map(|share| share.y[0].clone()).sum();
                let s_jk = shares.iter().map(|share| share.y[1].clone()).sum();
                let y = vec![d_jk, s_jk];
                (eid, Share { x, y })
            })
            .collect();
        let sum_pairs: HashMap<_, _> = sum_pairs
            .into_iter()
            .map(|(eid, sum)| (eid, Vec::from(&sum)))
            .collect();
        info!(self.shared.logger, "get_dsij_sum_pairs"; "the number of summation pairs (equals to eids) in this call" => sum_pairs.len());
        Ok(Response::new(DsijSumPairs { value: sum_pairs }))
    }

    async fn update_trustworthiness_with(&self, req: Request<Confidences>) -> ResponseResult<()> {
        let confidences = req.into_inner().value;
        for confidence in confidences.values() {
            if confidence.value.len() != 2 {
                return Err(Status::data_loss("Incorrect length of confidence array"));
            }
        }
        let core_r = self.shared.core.read().await;
        let weighted_sum: f64 = confidences
            .iter()
            .map(|(eid, confidence)| {
                let confidence = &confidence.value;
                let event = core_r.events.get(&eid).unwrap();
                match event.get_judge() {
                    Judge::True => confidence[0],
                    Judge::False => confidence[1],
                }
            })
            .sum();
        let tau = weighted_sum / confidences.len() as f64;
        drop(core_r);

        let mut core_w = self.shared.core.write().await;
        core_w.tau = tau.clone();
        info!(self.shared.logger, "update_trustworthiness_with"; "trustworthiness after this call" => tau);
        Ok(Response::new(()))
    }
}

#[tonic::async_trait]
impl rank_server::Rank for Arc<SlaveServer> {
    async fn get_tau_seq_shares(&self, req: Request<GroupInfo>) -> ResponseResult<TauSeqShares> {
        let GroupInfo { uid_gid_map } = req.into_inner();
        let groups: HashSet<_> = uid_gid_map.values().collect();
        let group_num = groups.len();
        let threshold = (group_num - 1) / 2 + 1;
        let pks = self.fetch_public_keys(true).await?.into_inner();

        // generate tau sequence
        let tau = self.shared.core.read().await.tau.clone();
        let tau = BigRational::from_f64(tau).unwrap();
        let mut tau_seq = Vec::with_capacity(group_num);
        let mut accumulator = BigRational::from_u8(1).unwrap();
        for _ in 0..group_num {
            accumulator *= tau.clone();
            tau_seq.push(accumulator.clone());
        }
        let tau_seq = tau_seq;

        // make shares
        let shares: Vec<Share<Rational>> = make_shares(&tau_seq, threshold as u8, group_num as u8);
        let shares: HashMap<_, _> = shares
            .into_iter()
            .map(|share| (share.x as Gid, Vec::from(&share)))
            .collect();

        let tau_seq_shares = pks
            .into_iter()
            .map(|(uid, pk)| {
                let gid = uid_gid_map.get(&uid).unwrap();
                let share = shares.get(&gid).unwrap();
                let share = ecies::encrypt(&pk, &share[..], &mut rand::thread_rng()).unwrap();
                (uid, share)
            })
            .collect();
        Ok(Response::new(TauSeqShares {
            value: tau_seq_shares,
        }))
    }

    async fn get_r_shares(&self, req: Request<GroupInfo>) -> ResponseResult<RShares> {
        let GroupInfo { uid_gid_map } = req.into_inner();
        let groups: HashSet<_> = uid_gid_map.values().collect();
        let group_num = groups.len();
        let threshold = (group_num - 1) / 2 + 1;
        let pks = self.fetch_public_keys(true).await?.into_inner();

        let r = vec![BigRational::from_u128(rand::random::<u128>())
            .unwrap_or(BigRational::from_u64(3).unwrap())];
        let shares = make_shares(&r, threshold as u8, group_num as u8);
        let shares: HashMap<_, _> = shares
            .into_iter()
            .map(|share| (share.x as Gid, Vec::from(&share)))
            .collect();

        let r_shares = pks
            .into_iter()
            .map(|(uid, pk)| {
                let gid = uid_gid_map.get(&uid).unwrap();
                let share = shares.get(&gid).unwrap();
                let share = ecies::encrypt(&pk, &share[..], &mut rand::thread_rng()).unwrap();
                (uid, share)
            })
            .collect();
        Ok(Response::new(RShares { value: r_shares }))
    }

    async fn get_h_set_shares(&self, req: Request<HSetSharesReq>) -> ResponseResult<HSetShares> {
        let HSetSharesReq {
            tau_seq_shares,
            r_shares,
            lambda,
        } = req.into_inner();
        // decrypt & deserialize
        let tau_seq_shares = tau_seq_shares.into_iter().map(|(tx_uid, share)| {
            let share =
                Share::<Rational>::try_from(&ecies::decrypt(&self.shared.sk, &share).unwrap()[..])
                    .unwrap();
            (tx_uid, share)
        });
        let r_shares = r_shares.into_iter().map(|(gid, share)| {
            let share =
                Share::<Rational>::try_from(&ecies::decrypt(&self.shared.sk, &share).unwrap()[..])
                    .unwrap();
            (gid, share)
        });

        // transform into BigRational
        use std::collections::BTreeMap;
        let lambda = BigRational::from_f64(lambda).unwrap();
        let r_shares: BTreeMap<Gid, BigRational> = r_shares
            .map(|(gid, share)| (gid, share.y[0].clone().into()))
            .collect();
        let tau_seqs: BTreeMap<Uid, Vec<BigRational>> = tau_seq_shares
            .map(|(tx_uid, share)| {
                let seq = share.y.iter().map(|x| x.clone().into()).collect(); // ascendingly ordered by gid
                (tx_uid, seq)
            })
            .collect(); // ascendingly ordered by uid

        let h_set: Vec<BigRational> = tau_seqs
            .iter()
            .map(|(_tx_uid, tau_seq)| {
                let gamma: BigRational = tau_seq
                    .iter()
                    .zip(r_shares.values())
                    .map(|(r_k, t_i_k)| r_k * t_i_k)
                    .sum();
                lambda.clone() * gamma
            })
            .collect();

        let groups: HashSet<_> = r_shares.keys().collect();
        let threshold = ((groups.len() - 1) / 2 + 1) as u8;
        let pks = self.fetch_public_keys(true).await?.into_inner();
        let client_n = pks.keys().count() as u8;
        let shares = make_shares(&h_set, threshold, client_n);
        let mut shares: Vec<_> = shares.into_iter().map(|share| Vec::from(&share)).collect();
        let shares: HashMap<Uid, _> = pks
            .into_iter()
            .map(|(uid, pk)| {
                let share = shares.pop().unwrap();
                let share = ecies::encrypt(&pk, &share[..], &mut rand::thread_rng()).unwrap();
                (uid, share)
            })
            .collect();

        Ok(Response::new(HSetShares { value: shares }))
    }

    async fn get_h_apo_set(&self, req: Request<HApoSetReq>) -> ResponseResult<HApoSet> {
        let gid_hshare_map = req.into_inner().value;
        let gid_hshare_map: HashMap<_, _> = gid_hshare_map
            .into_iter()
            .map(|(tx_gid, h_share)| {
                let h_share = Share::<Rational>::try_from(
                    &ecies::decrypt(&self.shared.sk, &h_share).unwrap()[..],
                )
                .unwrap();
                (tx_gid, h_share) // tx_gid, H(tx_gid, rx_uid)
            })
            .collect();

        let client_num = gid_hshare_map.iter().take(1).next().unwrap().1.y.len();
        let y: Vec<Rational> = (0..client_num)
            .map(|i| {
                gid_hshare_map
                    .iter()
                    .map(|(_tx_gid, share)| share.y[i].clone())
                    .sum()
            })
            .collect();

        let x = gid_hshare_map.iter().take(1).next().unwrap().1.x.clone();
        let h_apo_set = Share { x, y };
        Ok(Response::new(HApoSet {
            value: Vec::from(&h_apo_set),
        }))
    }
}

#[tonic::async_trait]
pub trait Registration {
    #[inline]
    fn generate_keypair() -> (SecretKey, PublicKey) {
        let mut rng = rand::thread_rng();
        ecies::generate_keypair(&mut rng)
    }

    fn judge(error_rate: f64, owned_event_records: Vec<RawEventRecord>) -> Vec<RawEventRecord> {
        use rand::distributions::Distribution;

        if owned_event_records.len() == 0 {
            return vec![];
        }
        let weights: Vec<_> = vec![error_rate, 1.0 - error_rate];
        let distribution =
            rand::distributions::weighted::alias_method::WeightedIndex::new(weights).unwrap();
        let mut rng = rand::thread_rng();
        let choices = [true, false];
        owned_event_records
            .into_iter()
            .map(|record| if choices[distribution.sample(&mut rng)] { RawEventRecord {
                identifier: record.identifier,
                delay_seconds: record.delay_seconds,
                owned: record.owned,
                claim: !record.claim
            }} else { record })
            .collect()
    }

    async fn register<P: AsRef<std::path::Path> + Sync + Send>(
        config: ClientConfig,
        log_path: Option<P>,
    ) -> anyhow::Result<SlaveServer> {
        let (sk, pk) = Self::generate_keypair();

        // register to get id(uid, gid)
        let mailbox = config.addrs.mailbox_addr.clone();

        // read local events
        use super::event::read_raw_event_records;
        let event_records = read_raw_event_records(&config.event_file_path, 0).await?;
        let owned_event_records: Vec<_> = event_records
            .into_iter()
            .filter(|record| record.owned)
            .collect();
        let owned_event_records = Self::judge(config.error_rate, owned_event_records);

        // request to register
        let server_url = config.addrs.remote_addr.clone();
        let mut reg_chan = registration_client::RegistrationClient::connect(server_url).await?;
        let RegistrationRes {
            uid,
            identifier_eid_map,
        } = reg_chan
            .register(RegistrationReq {
                pk: pk.to_bytes().to_vec(),
                mailbox,
                identifiers: owned_event_records
                    .iter()
                    .map(|record| record.identifier.clone())
                    .collect(),
            })
            .await?
            .into_inner();
        drop(reg_chan);

        // update events' eid
        let mut core = AlgoCore::new();
        owned_event_records
            .into_iter()
            .map(|record| {
                Event::new(
                    identifier_eid_map.get(&record.identifier).unwrap().clone(),
                    if record.claim {
                        Judge::True
                    } else {
                        Judge::False
                    },
                    record.owned,
                )
            })
            .for_each(|e| {
                core.events.insert(e.get_id(), e);
            });

        // let weights = vec![config.error_rate, 1.0 - config.error_rate];
        // let distribution =
        //     rand::distributions::weighted::alias_method::WeightedIndex::new(weights)?;

        // logger
        use slog::Drain;
        let owned_info = slog::o!("self.uid" => uid);
        let logger = match log_path {
            Some(path) => {
                // log to file
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(path)
                    .unwrap(); // will overwrite existing content

                let decorator = slog_term::PlainDecorator::new(file);
                let drain = slog_term::FullFormat::new(decorator).build().fuse();
                let drain = slog_async::Async::new(drain).build().fuse();
                slog::Logger::root(drain, owned_info)
            }
            None => {
                // log to terminal
                let decorator = slog_term::TermDecorator::new().build();
                let drain = slog_term::FullFormat::new(decorator).build().fuse();
                let drain = slog_async::Async::new(drain).build().fuse();
                slog::Logger::root(drain, owned_info)
            }
        };

        let shared = Shared {
            uid,
            core: RwLock::new(core),
            sk,
            logger: logger.clone(),
        };
        info!(logger, #"new node", "node constructed");

        Ok(SlaveServer {
            shared: Arc::new(shared),
            config,
        })
    }
}
impl Registration for SlaveServer {}

#[tonic::async_trait]
trait PublicKeyUtil {
    async fn fetch_public_keys(&self, online: bool) -> ResponseResult<HashMap<Uid, PublicKey>>;
}
#[tonic::async_trait]
impl PublicKeyUtil for Arc<SlaveServer> {
    async fn fetch_public_keys(&self, online: bool) -> ResponseResult<HashMap<Uid, PublicKey>> {
        let server_url = self.config.addrs.remote_addr.clone();
        let mut pk_chan = security_client::SecurityClient::connect(server_url)
            .await
            .unwrap();
        let pks = pk_chan
            .get_public_keys(PublicKeyQuery { online })
            .await?
            .into_inner()
            .value;
        drop(pk_chan);
        Ok(Response::new(
            pks.into_iter()
                .map(|(uid, pk)| {
                    let pk = PublicKey::from_bytes(&pk).unwrap();
                    (uid, pk)
                })
                .collect(),
        ))
    }
}
