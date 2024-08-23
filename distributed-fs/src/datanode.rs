use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use mp3::*;

fn main() {
    // Create folder to store files, if it doesn't already exist
    if !fs::metadata(FILES_PATH).is_ok() {
        let _ = fs::create_dir(FILES_PATH);
    }

    // Open file to read datanode ID
    let mut id_buf = String::new();
    let mut id_file = File::open(VM_ID_PATH).unwrap();
    let _ = id_file.read_to_string(&mut id_buf);
    let datanode_id: u8 = id_buf.trim().parse().unwrap();
    println!("Read datanode_id {}", datanode_id);

    let master = find_master();
    println!("Master: {}", master);

    let master = Arc::new(Mutex::new(master));

    // Listen for incoming connections
    let shared_master = master.clone();
    let client_listener = thread::spawn(move || client_listen(shared_master));

    // Block indefinitely
    client_listener.join().unwrap();
}

// Listener for client connections
fn client_listen(master: Arc<Mutex<u8>>) {
    let listen_addr = String::from("0:") + CD_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut opcode: [u8; 1] = [0];
            if stream.read_exact(&mut opcode).is_err() {
                continue;
            };

            match opcode[0] {
                // Query on current leader
                CD_GET_MASTER => {
                    let shared_master = master.clone();
                    thread::spawn(move || give_leader(stream, shared_master));
                }
                CD_WRITE_FILE => {
                    let shared_master = master.clone();
                    thread::spawn(move || receive_file(stream, shared_master));
                }
                CD_READ_FILE => {
                    let shared_master = master.clone();
                    thread::spawn(move || send_file(stream, shared_master));
                }
                _ => {}
            }
        } else {
            continue;
        }
    }
}

// CD Message 1 - Query for master node
fn give_leader(mut stream: TcpStream, shared_master: Arc<Mutex<u8>>) {
    let master: u8;
    {
        master = *shared_master.lock().unwrap();
    }
    let to_write: [u8; 1] = [master];
    stream.write(&to_write).unwrap();
}

// CD Message 2 - Receive file write from client
fn receive_file(mut stream: TcpStream, shared_master: Arc<Mutex<u8>>) {
    let filename = receive_filename(&mut stream).unwrap();
    println!("Receiving {}...", filename);

    // Read file from client
    let file: File = File::create(String::from(FILES_PATH) + &filename).unwrap();
    if let Err(_) = read_file_from_stream(&mut stream, file) {
        return;
    }

    // Send confirmation code to client
    if let Err(_) = stream.write(&CONFIRMATION) {
        return;
    }

    // Tell master that it received a file
    let master: u8;
    {
        master = *shared_master.lock().unwrap();
    }
    let sock_addr = String::from(VM_LIST[(master - 1) as usize]) + ":" + DM_PORT;
    let mut master_stream = TcpStream::connect(sock_addr).unwrap();
    master_stream.write(&[DM_FILE_RECEIVED]).unwrap();

    // Write the filename
    master_stream
        .write(&generate_filename_bytes(&filename))
        .unwrap();

    // Write self node id
    let id_str = fs::read_to_string(VM_ID_PATH).unwrap();
    let id: u8 = id_str.trim().parse().unwrap();
    master_stream.write(&[id]).unwrap();
}

// CD Message 3 - Send file write to client
fn send_file(mut stream: TcpStream, shared_master: Arc<Mutex<u8>>) {
    let filename = receive_filename(&mut stream).unwrap();
    println!("Sending {}...", filename);

    // Open file
    let file = match File::open(FILES_PATH.to_owned() + &filename) {
        Ok(file) => file,
        Err(_) => {
            return;
        }
    };

    // Actually send file to client
    if let Err(_) = send_file_over_stream(&mut stream, file) {
        return;
    }

    // Tell master that it sent a file
    let master: u8;
    {
        master = *shared_master.lock().unwrap();
    }
    let sock_addr = String::from(VM_LIST[(master - 1) as usize]) + ":" + DM_PORT;
    let mut master_stream = TcpStream::connect(sock_addr).unwrap();
    master_stream.write(&[DM_FILE_SENT]).unwrap();

    // Write the filename
    master_stream
        .write(&generate_filename_bytes(&filename))
        .unwrap();

    // Write self node id
    let id_str = fs::read_to_string(VM_ID_PATH).unwrap();
    let id: u8 = id_str.trim().parse().unwrap();
    master_stream.write(&[id]).unwrap();
}

// Routine to find master using lowest numbered number
// Note that the servers are numbered [1 - 10]!
fn find_master() -> u8 {
    for i in 0..VM_LIST.len() {
        let sock_addr: String = String::from(VM_LIST[i]) + ":" + DM_PORT;
        if let Ok(_) = TcpStream::connect(sock_addr) {
            return (i + 1) as u8;
        }
    }

    return 0;
}
