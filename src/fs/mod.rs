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

use std::fs::File;
use std::io::{Write, Seek, SeekFrom, Read};
use std::io;
use std::vec::Vec;
use crate::msg::{Msg, Header};
use byteorder::{ByteOrder, LittleEndian};


pub fn test() {
    println!("Hello, world!");
}


pub fn write_msg(mut f: File, msg: &Msg) -> io::Result<u64> {
//    let mut f = File::with_options().append(true).open(path)?;
    f.seek(SeekFrom::End(0))?;
    let position = f.stream_position()?;
    f.write(& msg.header.sign.to_le_bytes())?;
    f.write(& msg.header.hash.to_le_bytes())?;
    f.write(& msg.header.timestamp.to_le_bytes())?;
    f.write(& msg.header.size.to_le_bytes())?;
    f.write(&msg.payload)?;
    f.flush();
    Ok(position)
}

pub fn read_msg(mut f: File, pos: u64) -> io::Result<Msg> {
//    let mut f = File::with_options().read(true).open(path)?;
    f.seek(SeekFrom::Start(pos))?;
    macro_rules! load_part {
        ($size:literal) => {
               {
                    let mut buf = [0u8; $size];
                    f.read_exact(&mut buf).unwrap();
                    buf
                }
        }
    }
    let header = Header {
        sign: LittleEndian::read_u16(&load_part!(2)),
        hash: LittleEndian::read_u64(&load_part!(8)),
        timestamp: LittleEndian::read_u64(&load_part!(8)),
        size: LittleEndian::read_u32(&load_part!(4))
    };
    let mut buf = Vec::with_capacity(header.size as usize);
    let mut part_reader = f.take(header.size as u64);
    part_reader.read_to_end(&mut buf).unwrap();
    let msg = Msg{header, payload: buf.into()};
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use rand::{thread_rng, Rng};
    use rand::distributions::Alphanumeric;


    #[test]
    fn write_message() {
        let mut rng = rand::thread_rng();

        macro_rules! write_test {
        ($truncate:expr) => {{
            let mut tmpfile = File::with_options()
                .create(true).truncate($truncate).write(true)
                .open("/tmp/test.log").unwrap();
                let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(rng.gen_range(20, 100000))
                .collect();
            write_msg(tmpfile,
                             &Msg::new(String::from(rand_string).into_bytes())).unwrap()
          }}

        }
        macro_rules! read_test {
        ($pos:expr) => {{
            let mut tmpfile = File::with_options().read(true).open("/tmp/test.log").unwrap();
            let result: Msg = read_msg(tmpfile, $pos).unwrap();
            assert_eq!(result.is_valid(), true);
            }}
        }

        let mut count = 0u64;
        let mut vec:Vec<u64> = Vec::with_capacity(1000);

        loop {

            let pos = write_test!(count == 0u64);
            vec.push(pos);
            count += 1;
            println!("Writing element {}", count);
            if count == 1000 {
                break;
            }
        }

        for (index, pos) in vec.iter().enumerate() {
            println!("Reading element {} at pos: {:?}", index, pos);
            read_test!(*pos);
        }


    }

}