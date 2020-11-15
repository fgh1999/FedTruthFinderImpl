pub type Uid = i32;
pub type Gid = i32;

#[derive(Debug, Copy, Clone)]
pub struct Id {
    uid: Uid,
    gid: Gid,
}

#[allow(dead_code)]
impl Id {
    pub fn new(uid: Uid, gid: Gid) -> Id {
        Id { uid, gid }
    }

    pub fn get_uid(&self) -> Uid {
        self.uid
    }

    pub fn get_gid(&self) -> Gid {
        self.gid
    }
}

use std::sync::Mutex;
#[derive(Debug, Default)]
pub struct UidAssigner(Mutex<Uid>);

#[allow(dead_code)]
impl UidAssigner {
    pub fn new_uid(&self) -> Uid {
        let mut assigner = self.0.lock().unwrap();
        *assigner += 1;
        *assigner
    }
}