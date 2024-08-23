pub mod interface;
pub use interface::*;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Sender;

/********************* Metadata section *********************/

#[derive(Debug, PartialEq)]
pub enum FileState {
    Free,
    Read,
    Write,
}

#[derive(Debug)]
pub enum RequestType {
    Read,
    Write,
}

#[derive(Debug)]
pub struct FileSync {
    pub ops: Vec<u8>,
    pub state: FileState,
    pub queue: VecDeque<(RequestType, Sender<u8>)>,
}

impl FileSync {
    pub fn new() -> Self {
        FileSync {
            ops: Vec::new(),
            state: FileState::Free,
            queue: VecDeque::new(),
        }
    }

    // Start a write operation
    pub fn add_writer(&mut self, mut datanodes: Vec<u8>) {
        self.state = FileState::Write;
        self.ops.append(&mut datanodes);
    }

    // Start a read operation
    pub fn add_reader(&mut self, datanode: u8) {
        self.state = FileState::Read;
        self.ops.push(datanode);
    }

    // Called when datanode confirms write is complete
    pub fn write_complete(&mut self, datanode: u8) {
        let ops = &mut self.ops;
        for i in 0..ops.len() {
            if ops[i] == datanode {
                ops.remove(i);
                break;
            }
        }

        if ops.is_empty() {
            self.state = FileState::Free;
            match self.queue.pop_front() {
                Some((_, tx)) => {
                    tx.send(1).unwrap();
                }
                _ => {}
            }
        }
    }

    // Called when datanode confirms read is complete
    pub fn read_complete(&mut self, datanode: u8) {
        // Remove datanode from ops vector
        let ops = &mut self.ops;
        for i in 0..ops.len() {
            if ops[i] == datanode {
                ops.remove(i);
                break;
            }
        }

        // Check if we can let in another reader
        let mut pop_next: bool = false;
        match self.queue.front() {
            // Next in queue is write, do nothing
            Some((RequestType::Write, _)) => {}

            // Next in queue is read
            Some((RequestType::Read, _)) => {
                pop_next = true;
            }

            // None left in queue, only set state if no one else reading
            None => {
                // Current invocation is last in ops
                if self.ops.is_empty() {
                    self.state = FileState::Free;
                }
            }
        }

        // Actually pop from queue and send message down tx
        if pop_next {
            let (_, tx) = self.queue.pop_front().unwrap();
            tx.send(1).unwrap();
        }
    }
}

#[derive(Debug)]
pub struct Metadata {
    // Key is filename, Value is vector of vm numbers (1-indexed)
    pub files_storage: BTreeMap<String, Vec<u8>>,
    pub datanode_usage: BTreeMap<u8, Vec<String>>,
    pub files_sync: HashMap<String, FileSync>,
}

impl Metadata {
    // Initialise from a metadata file
    pub fn from(metadata_path: &str) -> Self {
        // Default is minimal datanode_usage with 10 VMs
        let mut datanode_usage: BTreeMap<u8, Vec<String>> = BTreeMap::new();
        for i in 1..11 {
            datanode_usage.insert(i, Vec::new());
        }

        // Attempt to open file, return empty if file does not exist
        let mut file = if let Ok(file) = File::open(metadata_path) {
            file
        } else {
            return Metadata {
                files_storage: BTreeMap::new(),
                datanode_usage,
                files_sync: HashMap::new(),
            };
        };

        let mut data = String::new();
        file.read_to_string(&mut data).unwrap();
        let mut files_storage: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        let mut files_sync: HashMap<String, FileSync> = HashMap::new();

        // Parse the metadata file
        for line in data.lines() {
            let (name, node) = if let Some((prefix, suffix)) = line.split_once(":") {
                (prefix, suffix)
            } else {
                continue;
            };

            let node: u8 = if let Ok(num) = node.parse() {
                num
            } else {
                continue;
            };

            // Add to file_storage
            files_storage
                .entry(String::from(name))
                .and_modify(|datanodes| datanodes.push(node))
                .or_insert(vec![node]);

            // Add to datanode_usage
            datanode_usage
                .entry(node)
                .and_modify(|files| files.push(String::from(name)))
                .or_insert(vec![String::from(name)]);

            // Add to writer_access_info
            files_sync
                .entry(String::from(name))
                .or_insert(FileSync::new());
        }

        Metadata {
            files_storage,
            datanode_usage,
            files_sync,
        }
    }

    // Sort the datanodes by load, return the n lowest
    // Taking into membership
    pub fn get_n_free_nodes(&self, mut n: usize, membership: Vec<u8>) -> Vec<u8> {
        let num_nodes = self.datanode_usage.len();
        if n > num_nodes {
            n = num_nodes;
        }

        let mut to_sort: Vec<(u8, usize)> = Vec::new();

        for i in 0..membership.len() {
            if membership[i] != 1 {
                continue;
            }
            let datanode_num: u8 = (i + 1) as u8;
            let entry = self.datanode_usage.get(&datanode_num).unwrap();
            to_sort.push((datanode_num, entry.len()));
        }

        to_sort.sort_by_key(|val| val.1);

        let mut output: Vec<u8> = Vec::new();
        for pair in &to_sort[0..n] {
            output.push(pair.0);
        }

        output
    }

    // Returns all datanodes storing specified file, or just [0] if file not in SDFS
    pub fn get_nodes_for_file(&self, filename: String) -> Vec<u8> {
        if let Some(nodes) = self.files_storage.get(&filename) {
            return nodes.to_vec();
        } else {
            return vec![0];
        }
    }

    // Add data for a new file to self
    pub fn add_file(&mut self, filename: String, node: u8) {
        // Add to file_storage
        self.files_storage
            .entry(filename.clone())
            .and_modify(|datanodes| {
                for node_used in datanodes.iter() {
                    if *node_used == node {
                        return;
                    }
                }
                datanodes.push(node);
            })
            .or_insert(vec![node]);

        // Add to datanode_usage
        self.datanode_usage
            .entry(node)
            .and_modify(|files| {
                for curr_file in files.iter() {
                    if *curr_file == filename {
                        return;
                    }
                }
                files.push(filename.clone())
            })
            .or_insert(vec![filename.clone()]);

        self.write_to_file();
    }

    // Writes current metadata to a file
    fn write_to_file(&self) {
        let mut file = File::create(METADATA_PATH).unwrap();
        for (filename, nodes) in self.files_storage.iter() {
            for node in nodes {
                let line = format!("{}:{}\n", filename, node);
                file.write(line.as_bytes()).unwrap();
            }
        }
        file.flush().unwrap();
    }
}

pub const METADATA_PATH: &str = "metadata";

pub const VM_ID_PATH: &str = "client_id.txt";

/********************* Membership section *********************/

// Addresses of all servers
pub const VM_LIST: [&str; 10] = [
    "fa23-cs425-5701.cs.illinois.edu",
    "fa23-cs425-5702.cs.illinois.edu",
    "fa23-cs425-5703.cs.illinois.edu",
    "fa23-cs425-5704.cs.illinois.edu",
    "fa23-cs425-5705.cs.illinois.edu",
    "fa23-cs425-5706.cs.illinois.edu",
    "fa23-cs425-5707.cs.illinois.edu",
    "fa23-cs425-5708.cs.illinois.edu",
    "fa23-cs425-5709.cs.illinois.edu",
    "fa23-cs425-5710.cs.illinois.edu",
];

// Membership list related
pub const MEMBERSHIP_PATH: &str = "sdfs/membership";
// pub const HEARTBEAT_PORT: &str = "43278";

pub fn get_membership() -> Vec<u8> {
    if !fs::metadata(MEMBERSHIP_PATH).is_ok() {
        // If the file doesn't exist, create an empty file
        let _ = fs::write(MEMBERSHIP_PATH, "");
    }
    let data = fs::read(MEMBERSHIP_PATH).unwrap();

    data
}

// Client ID related
pub const CLIENT_ID_PATH: &str = "client_id.txt";

/********************* Communications section *********************/

// Well-known ports
// D - Datanode; C - Client; M - Master;
// E.g. DC is Datanode -> Client
pub const DM_PORT: &str = "26777";
pub const CM_PORT: &str = "32339";
pub const CD_PORT: &str = "38333";

// Message types - CM
pub const CM_PUT_REQ: u8 = 1;
pub const CM_GET_REQ: u8 = 2;
pub const CM_DELETE_REQ: u8 = 3;
pub const CM_LS_REQ: u8 = 4;
pub const CM_MULTIREAD_REQ: u8 = 5;

// Message types - CD
pub const CD_GET_MASTER: u8 = 1;
pub const CD_WRITE_FILE: u8 = 2;
pub const CD_READ_FILE: u8 = 3;

// Message types - DM
pub const DM_FILE_RECEIVED: u8 = 1;
pub const DM_FILE_SENT: u8 = 2;

// Files path
pub const FILES_PATH: &str = "sdfs/files/";

// Buffer size for file send/receives
pub const BUF_SIZE: usize = 2048;

// Code used at end of file send/received
pub const CONFIRMATION: [u8; 4] = [0xDE, 0xAD, 0xBE, 0xEF];

// Reads 4 bytes off the stream and return the i32 formed
// Note: Consumes bytes in stream!!
pub fn read_payload_size(stream: &mut TcpStream) -> io::Result<i32> {
    let mut size_bytes: [u8; 4] = [0; 4];
    stream
        .read_exact(&mut size_bytes)
        .map(|_| i32::from_le_bytes(size_bytes))
}

// Writes 4 bytes onto the stream
// Note: Uses the stream!
pub fn write_payload_size(stream: &mut TcpStream, size: i32) -> io::Result<usize> {
    let size_bytes = size.to_le_bytes();
    stream.write(&size_bytes)
}

/********************* Misc utility functions section *********************/
pub fn generate_filename_bytes(filename: &str) -> Vec<u8> {
    let mut filename_bytes = String::from(filename).into_bytes();
    filename_bytes.insert(0, filename_bytes.len() as u8);
    filename_bytes
}

pub fn receive_filename(stream: &mut TcpStream) -> Result<String, String> {
    let mut filename_size: [u8; 1] = [0];
    if let Err(err) = stream.read_exact(&mut filename_size) {
        return Err(err.to_string());
    }
    let mut filename_bytes: Vec<u8> = vec![0; filename_size[0] as usize];
    if let Err(err) = stream.read_exact(&mut filename_bytes) {
        return Err(err.to_string());
    }
    let filename: String = String::from_utf8(filename_bytes).unwrap();
    Ok(filename)
}

// Buffered read of the stream into a file. Listens for confirmation after receiving
// 0-length buffer read.
pub fn read_file_from_stream(stream: &mut TcpStream, mut file: File) -> Result<(), String> {
    loop {
        let res = read_payload_size(stream);
        if let Err(err) = res {
            return Err(err.to_string());
        }
        let read_size = res.unwrap();

        // Check for end
        if read_size == 0 {
            break;
        }

        let mut buf: Vec<u8> = vec![0; read_size as usize];
        if let Err(err) = stream.read_exact(&mut buf) {
            return Err(err.to_string());
        }
        file.write(&buf).unwrap();
    }
    let _ = file.flush();

    let mut confirmation: [u8; 4] = [0; 4];
    match stream.read(&mut confirmation) {
        Ok(_) => {
            if confirmation == CONFIRMATION {
                return Ok(());
            } else {
                return Err("Failed to get file".to_string());
            }
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}

// Buffered write of the contents of file over stream. Each buffered write begins
// with the payload size. Once finished reading file, sends a CONFIRMATION message.
pub fn send_file_over_stream(mut stream: &mut TcpStream, mut file: File) -> Result<(), String> {
    let mut buf: [u8; BUF_SIZE] = [0; BUF_SIZE];
    while let Ok(bytes_read) = file.read(&mut buf) {
        // Write payload size
        if let Err(e) = write_payload_size(&mut stream, bytes_read as i32) {
            return Err(e.to_string());
        }

        // Stop if payload size is zero (done reading file)
        if bytes_read == 0 {
            break;
        }
        // Write payload itself
        if let Err(e) = stream.write(&buf[0..bytes_read]) {
            return Err(e.to_string());
        }
    }

    // Send confirmation code to client
    if let Err(e) = stream.write(&CONFIRMATION) {
        return Err(e.to_string());
    }

    return Ok(());
}
