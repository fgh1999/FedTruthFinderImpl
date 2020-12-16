use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonSet, GetFields};
use super::deamon_set::{DeamonOperation, DeamonOperations};
use crate::id::{Gid, Uid};
use num_rational::BigRational;
use num_traits::FromPrimitive;
use sharks::{secret_type::Rational, Share};
use std::collections::BTreeMap;

/// H(gid_{self}), ordered acsendingly by uid
type ResultType = Vec<BigRational>;

/// All the shares received by client $u_j$ should have the same `share.x`, i.e. $gid(u_j)$.
#[derive(Debug, Clone)]
pub enum HChannelPayload {
    /// A share of the random coefficient. This share's `y` should have a length of $1$.
    /// `Gid` indicates which group it was generated from.
    /// The number of this kind of payloads received by this deamon should be equal to the group number on the server, i.e. (2t+1)
    RandomCoefficientShare(Gid, Share<Rational>),

    /// A share of tau_sequence, containing a sequence like "tau_i(disturbed), tau_i^2(disturbed), ..., tau_i^(2t+1)(disturbed)"
    /// `Uid` indicates which client the "tau_i" belongs to.
    /// The number of this kind of payloads this deamon received should be equal to client_num in the configuration
    TauSequenceShare(Uid, Share<Rational>),
}

type ChannelPayload = HChannelPayload;

type HDeamon = Deamon<ResultType, ChannelPayload>; // threshold of this deamon should be group_n
#[tonic::async_trait]
impl DeamonOperation<ResultType, ChannelPayload> for HDeamon {
    async fn process(&self) -> Result<ResultType, DeamonError> {
        let group_n = self.get_threshold() as usize; // !!! 2t + 1
        let client_num = self.get_client_n() as usize;

        let mut random_coefficients_shares: BTreeMap<Gid, BigRational> = BTreeMap::new();
        let mut tau_sequence_shares: BTreeMap<Uid, BTreeMap<Gid, BigRational>> = BTreeMap::new();
        let rx_buffer = self.get_buffer();
        let mut buffer_lock = rx_buffer.lock().await;
        while tau_sequence_shares.len() < client_num
            || random_coefficients_shares.len() < group_n
        {
            match buffer_lock.recv().await {
                Some(payload) => {
                    match payload {
                        HChannelPayload::RandomCoefficientShare(gid, share) => {
                            let coef: BigRational = share.y[0].clone().into();
                            random_coefficients_shares.insert(gid, coef);
                        }
                        HChannelPayload::TauSequenceShare(uid, share) => {
                            let mut taus: BTreeMap<Gid, BigRational> = BTreeMap::new();
                            taus.extend(
                                share
                                    .y
                                    .iter()
                                    .enumerate()
                                    .map(|x| ((x.0 + 1) as Gid, x.1.clone().into())),
                            );
                            tau_sequence_shares.insert(uid, taus);
                        }
                    };
                }
                None => {
                    return Err(DeamonError::SharesNotEnough);
                }
            }
        }
        // TODO: check the len of coefs & tau_seq
        let lambda_g = BigRational::from_f64(self.get_lambda_g()).unwrap();
        let mut h_set = Vec::with_capacity(client_num);
        assert_eq!(client_num, tau_sequence_shares.len());
        for (_uid, t_i_seq) in tau_sequence_shares.iter() {
            assert_eq!(t_i_seq.len(), random_coefficients_shares.len());
            let gamma_i: BigRational = random_coefficients_shares
                .values()
                .zip(t_i_seq.values())
                .map(|(r_k, t_i_k)| r_k * t_i_k)
                .sum();
            let h_i = lambda_g.clone() * gamma_i;
            h_set.push(h_i);
        }
        Ok(h_set)
    }
}

pub type HDeamonSet = DeamonSet<ResultType, ChannelPayload>;
impl DeamonOperations<ResultType, ChannelPayload> for HDeamonSet {}
