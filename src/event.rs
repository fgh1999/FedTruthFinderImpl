use std::fmt;
use std::sync::Mutex;

pub type Eid = u64;
pub type EventIdentifier = String;

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Copy, Clone)]
pub enum Judge {
    False,
    True,
}

impl fmt::Display for Judge {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Judge::False => write!(f, "Judge::False"),
            Judge::True => write!(f, "Judge::True"),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq, Copy, Clone)]
pub struct Event {
    id: Eid,
    judge: Judge,
    owned: bool,
}

#[allow(dead_code)]
impl Event {
    #[inline]
    pub fn new(id: Eid, judge: Judge, owned: bool) -> Event {
        Event { id, judge, owned }
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
    pub fn is_owned(&self) -> bool {
        self.owned
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
    pub owned: bool,
    pub claim: bool,
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
    let mut res = Vec::new();
    if limitation == 0 {
        for record in rdr.deserialize() {
            let record: RawEventRecord = record?;
            res.push(record);
        }
    } else {
        for record in rdr.deserialize() {
            if res.len() < limitation {
                let record: RawEventRecord = record?;
                res.push(record);
            } else {
                break;
            }
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
        tokio::time::sleep(duration).await;
    }
}
