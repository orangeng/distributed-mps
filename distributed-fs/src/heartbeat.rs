use mp3::*;

use std::fs;
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::Duration;

fn main() {
    // Spawn query thread
    let query_handle = thread::spawn(query);

    query_handle.join().unwrap();
}

fn query() {
    let mut membership: [u8; VM_LIST.len()] = [0; VM_LIST.len()];
    write(&membership);
    loop {
        for i in 0..VM_LIST.len() {
            let socket_addr = String::from(VM_LIST[i]) + ":" + CD_PORT;
            let first_addr = if let Ok(mut iter) = socket_addr.to_socket_addrs() {
                iter.next().unwrap()
            } else {
                continue;
            };

            match TcpStream::connect_timeout(&first_addr, Duration::from_secs(2)) {
                Ok(_) => {
                    if membership[i] != 1 {
                        membership[i] = 1;
                        write(&membership);
                    }
                }
                Err(_) => {
                    if membership[i] != 0 {
                        membership[i] = 0;
                        write(&membership);
                    }
                }
            }
        }
    }
}

fn write(status: &[u8]) {
    fs::write(MEMBERSHIP_PATH, status).unwrap();
}
