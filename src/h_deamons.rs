use super::deamon_error::DeamonError;
use super::deamon_set::{Deamon, DeamonSet, GetFields};
use super::deamon_set::{DeamonOperation, DeamonOperations};
use super::id::{Uid, Gid};
use sharks::{secret_type::Rational, Share};
use num_rational::BigRational;
use num_traits::FromPrimitive;
use std::collections::BTreeMap;

/// H(gid_{self}), ordered acsendingly by uid
type ResultType = Vec<BigRational>; 

/// All the shares received by client $u_j$ should have the same `share.x`, i.e. $gid(u_j)$.
#[derive(Debug, Clone)]
pub enum HChannelPayload {
    /// A share of the random coefficient.
    /// This share's `y` should have a length of $1$.
    /// The number of this kind of payloads this deamon received should be equal to the group number on the server, i.e. (2t+1) 
    RandomCoefficientShare(Gid, Share<Rational>),

    /// A share of tau_sequence, containing a sequence like "tau_i(disturbed), tau_i^2(disturbed), ..., tau_i^(2t+1)(disturbed)"
    /// The number of this kind of payloads this deamon received should be equal to client_num in the configuration
    TauSequenceShare(Uid, Share<Rational>),
}

type ChannelPayload = HChannelPayload;

fn generate_lambda_sequence(group_n: usize) -> Vec<BigRational> {
    use nalgebra::DMatrix;
    use num_traits::pow;
    let dm = DMatrix::from_fn(group_n, group_n, |i, j| pow(j as f64 + 1.0, i));
    let dm = dm.try_inverse().unwrap();
    let dm = dm.row(0);
    dm.column_iter()
        .map(|col| {
            let lambda = col[(0, 0)];
            BigRational::from_f64(lambda).unwrap()
        })
        .collect()
}


type HDeamon = Deamon<ResultType, ChannelPayload>; // threshold of this deamon should be group_n
#[tonic::async_trait]
impl DeamonOperation<ResultType, ChannelPayload> for HDeamon {
    async fn process(&self) -> Result<ResultType, DeamonError> {
        let group_n = self.get_threshold() as usize; // !!! 2t + 1
        let client_num = self.get_client_n();
        
        let mut random_coefficients_shares: BTreeMap<Gid, BigRational> = BTreeMap::new();
        let mut tau_sequence_shares: BTreeMap<Uid, BTreeMap<Gid, BigRational>> = BTreeMap::new();
        let rx_buffer = self.get_buffer();
        let mut buffer_lock = rx_buffer.lock().await;
        while random_coefficients_shares.len() < group_n || tau_sequence_shares.len() < 1 {
            match buffer_lock.recv().await {
                Some(payload) => {
                    match payload {
                        HChannelPayload::RandomCoefficientShare(gid, share) => { 
                            let coef: BigRational = share.y[0].clone().into();
                            random_coefficients_shares.insert(gid, coef);
                        },
                        HChannelPayload::TauSequenceShare(uid, share) => {
                            let mut taus: BTreeMap<Gid, BigRational> = BTreeMap::new();
                            taus.extend(share.y.iter().enumerate().map(|x| (x.0 as Gid + 1, x.1.clone().into())));
                            tau_sequence_shares.insert(uid, taus);
                        },
                    };
                }
                None => {
                    return Err(DeamonError::SharesNotEnough);
                }
            }
        }
        // TODO: check the len of coefs & tau_seq

        let lambda_seq = generate_lambda_sequence(group_n);
        let mut h_set = Vec::with_capacity(client_num as usize);
        for i in 1..=client_num {
            let t_i_seq = tau_sequence_shares.get(&i).unwrap();
            let gamma_i: BigRational = random_coefficients_shares.iter().zip(t_i_seq.iter()).map(|(r_k, t_i_k)| r_k.1 * t_i_k.1).sum();
            // {
            //     let mut gamma_i = BigRational::from_u64(0).unwrap();
            //     for j in 1..=client_num {
            //         gamma_i += random_coefficients_shares.get(&j).unwrap() + t_i_seq.get(&j).unwrap();
            //     }
            //     gamma_i
            // };
            let h_i = lambda_seq[i as usize - 1].clone() * gamma_i;
            h_set.push(h_i);
        }

        Err(DeamonError::Unimplemented("".into()))
    }
}

pub type HDeamonSet = DeamonSet<ResultType, ChannelPayload>;
impl DeamonOperations<ResultType, ChannelPayload> for HDeamonSet {}
