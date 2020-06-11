/*
 *  Copyright (c)  2020,  Marco Sarti <marco.sarti at gmail dot com>
 *  All rights reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of Redis nor the names of its contributors may be used
 *      to endorse or promote products derived from this software without
 *      specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 *
 */
use uuid::Uuid;
use crate::err::{Result, Error};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::time::{SystemTime};
use std::path::{PathBuf, Path};
use std::fs::{create_dir_all, metadata};
use crate::err::ErrorKind::LogFileAlreadyPresent;

const MSG_SIGN:u16 = 0xAFAF;
const BASE_PATH:&str = "/tmp";

pub struct IndexRecord {
    pub id: u64,
    pub pos: u64
}

pub struct Header {
    pub sign: u16,
    pub hash: u64,
    pub timestamp: u64,
    pub size: u32,
}

pub struct MsgLog {
    topic: String,
    partition: u16,
    segment: u16,
    base_path: String,
}

pub struct Msg {
    pub header: Header,
    pub payload: Vec<u8>
}

impl Msg {

    pub fn is_valid(&self) -> bool {
        self.header.sign == MSG_SIGN &&
            self.header.size as usize == self.payload.len() &&
            self.header.hash == Msg::calculate_hash(self.header.sign,
                                                    self.header.timestamp, &self.payload)
    }

    /// Calculates the hash of this message
    pub fn calculate_hash(header: u16, timestamp: u64, payload: &Vec<u8>) -> u64 {
        let mut hasher = DefaultHasher::new();
        hasher.write_u16(header);
        hasher.write_u64(timestamp);
        hasher.write(payload);
        hasher.finish()
    }


    pub fn new(payload: Vec<u8>) -> Self {
        let new_payload = payload.clone();
        let now =
            match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                _ => panic!()
            };
        let calculated_hash = Msg::calculate_hash(MSG_SIGN, now, &payload);
        let size:u32 = new_payload.len() as u32;

        Msg { header: Header{sign: MSG_SIGN, hash: calculated_hash, timestamp: now, size},
            payload: new_payload }
    }


}

impl MsgLog {
    pub fn new(basedir: Option<String>, topic: String, partition: u16, segment: u16) -> Self {
        MsgLog{base_path: basedir.unwrap_or(BASE_PATH.to_string()), topic, partition, segment}
    }

    pub fn get_path(&self) -> String {
        format!("{}/{}/{:08}", self.base_path, self.topic, self.partition)
    }

    pub fn get_file(&self) -> String {
        format!("{}/{}/{:08}/{:08}.log", self.base_path, self.topic, self.partition, self.segment)
    }


    pub fn create_new(&self) -> Result<()>{
        create_dir_all(Path::new(self.get_path().as_str()))?;
        let str_file_path = self.get_file();
        let file_path = Path::new(str_file_path.as_str());
        if file_path.exists() && metadata(self.get_file().as_str())?.len() > 0 {
            return Err(Error::from(LogFileAlreadyPresent))
        }
        Ok(())
    }

}



pub fn new_id() -> Result<String> {
    let new_id = Uuid::new_v4();
    Ok(new_id.to_simple().to_string())
}
