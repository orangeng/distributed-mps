use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Mutex};
use std::thread;

use mp3::*;

fn main() {
    let metadata = Metadata::from(METADATA_PATH);
    let shared_meta = Arc::new(Mutex::new(metadata));

    // Listen for incoming connections
    let shared_meta_client = shared_meta.clone();
    let client_listener = thread::spawn(move || client_listen(shared_meta_client));

    let shared_meta_datanode = shared_meta.clone();
    let datanode_listener = thread::spawn(move || datanode_listen(shared_meta_datanode));

    client_listener.join().unwrap();
    datanode_listener.join().unwrap();
}

// <----------- Stream Listener Functions ----------->
// Listener for client connections
fn client_listen(shared_meta: Arc<Mutex<Metadata>>) {
    let listen_addr = String::from("0:") + CM_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut opcode: [u8; 1] = [0];
            if stream.read_exact(&mut opcode).is_err() {
                continue;
            };

            match opcode[0] {
                // Request to put file
                CM_PUT_REQ => {
                    let shared_meta_handle = shared_meta.clone();

                    thread::spawn(move || handle_put_file(stream, shared_meta_handle));
                }
                // Request to get file
                CM_GET_REQ => {
                    let shared_meta_handle = shared_meta.clone();

                    thread::spawn(move || handle_get_file(stream, shared_meta_handle));
                }
                // Request to list where file is stored
                CM_LS_REQ => {
                    let shared_meta_handle = shared_meta.clone();

                    thread::spawn(move || handle_ls_file(stream, shared_meta_handle));
                }

                _ => {}
            }
        } else {
            continue;
        }
    }
}

// Listener for datanode connections
fn datanode_listen(shared_meta: Arc<Mutex<Metadata>>) {
    let listen_addr = String::from("0:") + DM_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut opcode: [u8; 1] = [0];
            if stream.read_exact(&mut opcode).is_err() {
                continue;
            };

            // Idiom is to pass the stream off to another thread that will handle it
            match opcode[0] {
                DM_FILE_RECEIVED => {
                    let shared_meta_clone = shared_meta.clone();
                    thread::spawn(move || datanode_file_received(stream, shared_meta_clone));
                }
                DM_FILE_SENT => {
                    let shared_meta_clone = shared_meta.clone();
                    thread::spawn(move || datanode_file_sent(stream, shared_meta_clone));
                }
                _ => {}
            }
        } else {
            continue;
        }
    }
}

fn datanode_file_received(mut stream: TcpStream, shared_meta: Arc<Mutex<Metadata>>) {
    let filename = receive_filename(&mut stream).unwrap();

    // Receive the node number
    let mut node_num: [u8; 1] = [0];
    stream.read_exact(&mut node_num).unwrap();

    println!("Datanode {} received {}.", node_num[0], filename);

    // Update Metadata
    {
        let mut meta = shared_meta.lock().unwrap();
        meta.files_sync
            .get_mut(&filename)
            .unwrap()
            .write_complete(node_num[0]);
        meta.add_file(filename.clone(), node_num[0]);
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }
}

fn datanode_file_sent(mut stream: TcpStream, shared_meta: Arc<Mutex<Metadata>>) {
    let filename = receive_filename(&mut stream).unwrap();

    // Receive the node number
    let mut node_num: [u8; 1] = [0];
    stream.read_exact(&mut node_num).unwrap();

    println!("Datanode {} received {}.", node_num[0], filename);

    // Update Metadata
    {
        let mut meta = shared_meta.lock().unwrap();
        meta.files_sync
            .get_mut(&filename)
            .unwrap()
            .read_complete(node_num[0]);
        meta.add_file(filename.clone(), node_num[0]);
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }
}

// <----------- Stream Listener Helper Functions ----------->
// CM Message 1 - Request to put file
fn handle_put_file(mut stream: TcpStream, shared_meta: Arc<Mutex<Metadata>>) {
    let mut client_id: [u8; 1] = [0];
    stream.read_exact(&mut client_id).unwrap();
    let client_id = client_id[0];

    let mut no_datanodes: [u8; 1] = [0];
    stream.read_exact(&mut no_datanodes).unwrap();
    let no_datanodes = no_datanodes[0];

    let filename = receive_filename(&mut stream).unwrap();

    println!(
        "received request from {} to put file {} into {} datanodes",
        client_id, filename, no_datanodes
    );

    // Do checks
    let mut rx_opt: Option<Receiver<u8>> = None;
    let mut file_exist: bool = false;
    {
        let mut meta = shared_meta.lock().unwrap();

        match meta.files_sync.get_mut(&filename) {
            // File does exist
            Some(file_sync) => {
                file_exist = true;
                if file_sync.state != FileState::Free {
                    let (tx, rx) = mpsc::channel::<u8>();
                    file_sync.queue.push_back((RequestType::Write, tx));
                    rx_opt = Some(rx);
                }
            }
            // File does not exist
            None => {
                meta.files_storage.insert(filename.clone(), Vec::new());
                meta.files_sync.insert(filename.clone(), FileSync::new());
            }
        }
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }

    // Wait if necessary
    if let Some(rx) = rx_opt {
        rx.recv().unwrap();
    }

    let mut list: Vec<u8>;
    {
        let mut meta = shared_meta.lock().unwrap();

        // Get n free nodes if file does not exist
        if !file_exist {
            list = meta.get_n_free_nodes(no_datanodes.into(), get_membership());
        }
        // Get n existing nodes if file already exists
        else {
            list = meta.get_nodes_for_file(filename.clone())[0..(no_datanodes as usize)].into();
        }
        meta.files_sync
            .get_mut(&filename)
            .unwrap()
            .add_writer(list.clone());
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }

    list.insert(0, list.len() as u8);
    stream.write(list.as_slice()).unwrap();
}

// CM Message 2 - Request to get file
fn handle_get_file(mut stream: TcpStream, shared_meta: Arc<Mutex<Metadata>>) {
    let mut client_id: [u8; 1] = [0];
    stream.read_exact(&mut client_id).unwrap();
    let client_id = client_id[0];

    let filename = receive_filename(&mut stream).unwrap();

    println!(
        "received request from {} to get file {}",
        client_id, filename
    );

    // Do checks
    let mut rx_opt: Option<Receiver<u8>> = None;
    {
        let mut meta = shared_meta.lock().unwrap();

        match meta.files_sync.get_mut(&filename) {
            // File does exist
            Some(file_sync) => {
                if file_sync.state != FileState::Free {
                    if file_sync.state == FileState::Write {
                        let (tx, rx) = mpsc::channel::<u8>();
                        file_sync.queue.push_back((RequestType::Read, tx));
                        rx_opt = Some(rx);
                    } else if file_sync.state == FileState::Read {
                        if file_sync.queue.len() > 0 || file_sync.ops.len() >= 2 {
                            let (tx, rx) = mpsc::channel::<u8>();
                            file_sync.queue.push_back((RequestType::Read, tx));
                            rx_opt = Some(rx);
                        }
                    }
                }
            }
            // File does not exist
            None => {}
        }
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }

    // Wait if necessary
    if let Some(rx) = rx_opt {
        rx.recv().unwrap();
    }

    let to_write_datanode: [u8; 1];
    {
        let mut meta = shared_meta.lock().unwrap();
        to_write_datanode = [meta.get_nodes_for_file(filename.clone())[0]];
        meta.files_sync
            .get_mut(&filename)
            .unwrap()
            .add_reader(to_write_datanode[0].clone());
        println!(
            "FileSync: {:?}",
            meta.files_sync.get_mut(&filename).unwrap()
        );
    }

    stream.write(&to_write_datanode).unwrap();
}

// CM Message 2 - Request to get file
fn handle_ls_file(mut stream: TcpStream, shared_meta: Arc<Mutex<Metadata>>) {
    let filename = receive_filename(&mut stream).unwrap();

    println!("received request from to list file {}", filename);

    let meta = shared_meta.lock().unwrap();
    let mut to_list_datanodes = meta.get_nodes_for_file(filename);

    to_list_datanodes.insert(0, to_list_datanodes.len() as u8);
    stream.write(&to_list_datanodes).unwrap();
}
