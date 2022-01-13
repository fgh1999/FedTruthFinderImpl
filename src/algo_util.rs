use num_rational::BigRational;
use num_traits::cast::FromPrimitive;
use sharks::{secret_type::Rational, SecretSharingOperation, Share};

pub fn make_shares(secrets: &[BigRational], t: u8, n: u8) -> Vec<Share<Rational>> {
    let mut shark = sharks::Sharks::new();
    shark.dealer(t, secrets).take(n as usize).collect()
}

pub fn generate_dsij_shares(
    dsij_pair: (f64, f64),
    threshold: usize,
    client_n: usize,
) -> Vec<Share<Rational>> {
    let threshold = threshold as u8;
    let client_n = client_n as u8;
    let dsij_pair = vec![
        BigRational::from_f64(dsij_pair.0).unwrap(),
        BigRational::from_f64(dsij_pair.1).unwrap(),
    ];
    make_shares(&dsij_pair, threshold, client_n)
}
