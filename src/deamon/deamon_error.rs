use crate::id::Uid;
use tokio::time::Elapsed;

type Threshold = u8;

#[derive(Debug, thiserror::Error)]
pub enum DeamonError {
    #[error("shares not enough")]
    SharesNotEnough,

    #[error("timeout")]
    Timeout(#[from] Elapsed),

    #[error("result can not be converted into f64 validly")]
    InvalidConversion,

    #[error("shares cannot be added")]
    SharesCannotAdded,

    #[error("thresholds is inconsistent: given {given}, while exists {existed}")]
    ThresholdInconsistency {
        given: Threshold,
        existed: Threshold,
    },

    #[error("client_n is inconsistent: given {given}, while exists {existed}")]
    ClientNumInconsistency { given: Uid, existed: Uid },

    #[error("{0}")]
    Unimplemented(String),
}

impl DeamonError {
    pub fn check_threshold_consistency(
        given: Threshold,
        existed: Threshold,
    ) -> Result<(), DeamonError> {
        if given != existed {
            Err(DeamonError::ThresholdInconsistency { given, existed })
        } else {
            Ok(())
        }
    }

    pub fn check_client_num_consistency(given: Uid, existed: Uid) -> Result<(), DeamonError> {
        if given != existed {
            Err(DeamonError::ClientNumInconsistency { given, existed })
        } else {
            Ok(())
        }
    }
}
