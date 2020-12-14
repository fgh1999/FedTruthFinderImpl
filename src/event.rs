use rand::distributions::weighted::alias_method::{Weight, WeightedIndex};
use rand::prelude::*;
use std::option::Option;
use std::sync::Mutex;

pub type Eid = u64;
pub type EventIdentifier = String;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Copy, Clone)]
pub enum Judge {
    False,
    True,
}

impl<W: Weight> Distribution<Judge> for WeightedIndex<W> {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Judge {
        match self.sample(rng) {
            0 => Judge::False,
            _ => Judge::True,
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Copy, Clone)]
pub struct Event {
    id: Eid,
    judge: Judge,
    confidence: Option<f64>,
}

#[allow(dead_code)]
impl Event {
    #[inline]
    pub fn new(id: Eid, judge: Judge) -> Event {
        Event {
            id,
            judge,
            confidence: None,
        }
    }

    #[inline]
    pub fn with_confidence(id: Eid, judge: Judge, confidence: f64) -> Event {
        Event {
            id,
            judge,
            confidence: Some(confidence),
        }
    }

    #[inline]
    pub fn get_judge(&self) -> Judge {
        self.judge
    }

    #[inline]
    pub fn get_id(&self) -> Eid {
        self.id
    }

    #[inline]
    pub fn get_confidence(&self) -> Option<f64> {
        self.confidence
    }

    #[inline]
    pub fn set_confidence(&mut self, confidence: f64) {
        self.confidence = Some(confidence);
    }
}

#[derive(Debug, Default)]
pub struct EidAssigner(Mutex<Eid>);

#[allow(dead_code)]
impl EidAssigner {
    pub fn new_id(&self) -> Eid {
        let mut assigner = self.0.lock().unwrap();
        *assigner += 1;
        *assigner
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct RawEventRecord {
    pub identifier: EventIdentifier,
    pub delay_seconds: f64,
}

use tokio::sync::broadcast::{Receiver, Sender};

pub type EventIdentifierReceiver = Receiver<EventIdentifier>;
pub type EventIdentifierSender = Sender<EventIdentifier>;

/// read raw event records from some csv files with a maximum records number limitation
pub async fn read_raw_event_records<P: AsRef<std::path::Path>>(
    path: P,
    limitation: usize,
) -> anyhow::Result<Vec<RawEventRecord>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut res = Vec::with_capacity(limitation);
    for record in rdr.deserialize() {
        let record: RawEventRecord = record?;
        if res.len() < limitation {
            res.push(record);
        } else {
            break;
        }
    }
    Ok(res)
}

pub async fn dispatch_event_identifier_from<T>(
    raw_event_record_iter: T,
    dispatcher: EventIdentifierSender,
) where
    T: Iterator<Item = RawEventRecord>,
{
    for record in raw_event_record_iter {
        let duration = record.delay_seconds;
        let duration = tokio::time::Duration::from_secs_f64(duration);
        dispatcher.send(record.identifier).unwrap();
        tokio::time::delay_for(duration).await;
    }
}
