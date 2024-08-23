use lazy_static::lazy_static;
use rand::Rng;
use utils::*;

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::{self, Write};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{process, thread};

extern crate utils;

struct MemListEntry {
    hostname: String,
    port: u16,
    timestamp: u64,
    heartbeat: u32,
    local_time: SystemTime,
    status: u8, // 0 - Alive, 1 - Failed, 2 - Suspected
    inc_num: u32,
}

struct Mode {
    // 0 - Normal, 1 - Suspicion
    mode: u8,
    last_changed: SystemTime,
}

lazy_static! {
    static ref LOG_FILE: File = get_logfile();
}

fn main() {
    // Initialise self ID
    let hostname: String = get_hostname();
    let curr_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let timestamp: u64 = curr_time.as_secs();
    let self_id = generate_id(&hostname, HEARTBEAT_PORT, timestamp);
    log("Created Self ID: ".to_string() + &self_id);

    // Creating membership list
    let mut mem_list_hm: HashMap<String, MemListEntry> = HashMap::with_capacity(14); //  With 10 as 75% load

    // Creating mem list entry for this machine
    let self_entry = MemListEntry {
        hostname: hostname.clone(),
        port: utils::HEARTBEAT_PORT,
        timestamp: timestamp,
        heartbeat: 1,
        local_time: SystemTime::now(),
        status: 0,
        inc_num: 1,
    };
    mem_list_hm.insert(self_id.clone(), self_entry);

    // Creating thread-safe wrapper for mem_list
    let mem_list: Arc<Mutex<HashMap<String, MemListEntry>>> = Arc::new(Mutex::new(mem_list_hm));

    // Gossip vs Gossip-Suspicion mode
    let mode: Mode = Mode {
        mode: 0,
        last_changed: SystemTime::now(),
    };
    let mode: Arc<Mutex<Mode>> = Arc::new(Mutex::new(mode));

    // Check if this machine is the introducer
    if INTRO_HOSTNAME != hostname {
        // Reach out to introducer if currently NOT the introducer, send mem_list that only has self
        let mem_list_hm = mem_list.lock().unwrap();
        send_mem_list(INTRO_HOSTNAME, HEARTBEAT_PORT, &mem_list_hm, 0);
    }

    // Spawn fail timeout checker thread
    let mem_list_clone_for_timeout = mem_list.clone();
    let mode_clone_timeout = mode.clone();
    let self_id_clone_for_timeout = self_id.clone();
    // let to_leave_for_timeout = to_leave.clone();
    let timeout_handle = thread::spawn(move || {
        check_timeout(
            mem_list_clone_for_timeout,
            mode_clone_timeout,
            self_id_clone_for_timeout,
        )
    });

    // Spawn UDP listener thread
    let mem_list_clone_for_listen = mem_list.clone();
    let mode_clone_listen = mode.clone();
    let self_id_clone_for_listen = self_id.clone();
    let listener_handle = thread::spawn(move || {
        update_membership(
            mem_list_clone_for_listen,
            mode_clone_listen,
            self_id_clone_for_listen,
        )
    });

    // Spawn gossiper thread
    let mem_list_clone_for_gossiper = mem_list.clone();
    let mode_clone_gossiper = mode.clone();
    let self_id_clone_for_gossiper = self_id.clone();
    let gossip_handle = thread::spawn(move || {
        gossip(
            mem_list_clone_for_gossiper,
            mode_clone_gossiper,
            self_id_clone_for_gossiper,
        )
    });

    loop {
        println!("Enter a command:");
        let mut input = String::new();

        // Read user input
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        // Trim leading/trailing whitespace and convert to lowercase
        let command = input.trim().to_lowercase();

        // Match the command and call the corresponding function
        match command.as_str() {
            "leave" => {
                leave(mem_list, self_id);
                break;
            }
            "list_mem" => list_mem(&mem_list, &mode),
            "list_self" => list_self(&self_id),
            "enable suspicion" => toggle_suspicion(&mode, 1),
            "disable suspicion" => toggle_suspicion(&mode, 0),
            "exit" => process::exit(1), // Exit the loop on "exit" command
            _ => println!("Invalid command. Try again."),
        }
    }

    // Wait for threads to terminate following leave
    timeout_handle.join().unwrap();
    listener_handle.join().unwrap();
    gossip_handle.join().unwrap();
}

// <------------------------ USER COMMAND FUNCTIONS ------------------------>

fn leave(mem_list_arc: Arc<Mutex<HashMap<String, MemListEntry>>>, self_id: String) {
    log("Leave function called".to_string());

    // Self self heartbeat to 0
    let mut mem_list = mem_list_arc.lock().unwrap();
    let self_entry = mem_list.get_mut(&self_id).unwrap();
    self_entry.heartbeat = 0;
    drop(mem_list);

    log("Peer has left the network gracefully.".to_string());
    println!("Peer has left the network gracefully.");
}

fn list_mem(mem_list: &Arc<Mutex<HashMap<String, MemListEntry>>>, mode: &Arc<Mutex<Mode>>) {
    // Print current mode
    let mode = mode.lock().unwrap();
    println!("Mode: {}", mode.mode);

    println!("Here are the elements in the current membership list: ");

    println!(
        "{0: <50} | {1: <10} | {2: <10} | {3: <10} | {4: <10}",
        "ID", "Heartbeat", "Local time", "Status", "Inc Num"
    );

    // Acquire a lock on the Mutex
    let locked_mem_list = mem_list.lock().unwrap();

    // Iterate over the HashMap and print its elements
    for (key, value) in locked_mem_list.iter() {
        let local_time = value
            .local_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        println!(
            "{0: <50} | {1: <10} | {2: <10} | {3: <10} | {4: <10}",
            key, value.heartbeat, local_time, value.status, value.inc_num
        );
    }
}

fn list_self(self_id: &String) {
    println!("The current machine's ID is {}", self_id);
}

// Toggle suspicion mode, as controlled by user
fn toggle_suspicion(mode: &Arc<Mutex<Mode>>, to_set: u8) {
    let mut mode = mode.lock().unwrap();

    mode.mode = to_set;
    mode.last_changed = SystemTime::now();
    if to_set == 0 {
        log("Suspicion disabled.".to_string());
        println!("Suspicion disabled.");
    } else if to_set == 1 {
        log("Suspicion enabled".to_string());
        println!("Suspicion enabled.");
    }
}

// <------------------------ UTILITY FUNCTIONS ------------------------>
// Creates file for logging
// Requires the file "../../self_logname.txt" to hold logging file's pathname
fn get_logfile() -> File {
    let mut file = File::open("self_logname.txt").unwrap();
    let mut logname = String::new();
    file.read_to_string(&mut logname).unwrap();
    return File::create(logname.trim()).expect("Unable to open file for logging");
}

// Reads hostname from "self_hostname.txt"
// Requires the file "../../self_hostname.txt" to hold own hostname
fn get_hostname() -> String {
    let mut file = File::open("self_hostname.txt").unwrap();
    let mut self_hostname = String::new();
    file.read_to_string(&mut self_hostname).unwrap();
    let hostname: String = String::from(self_hostname.trim());
    hostname
}

// Generate unique ID
fn generate_id(hostname: &String, port: u16, timestamp: u64) -> String {
    String::from(hostname) + ":" + port.to_string().as_str() + ":" + timestamp.to_string().as_str()
}

// Logging utility function, write logging message to logfile
fn log(msg: String) {
    let curr_time = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();
    let timestamp: u64 = curr_time.as_secs();
    let write_res = write!(&*LOG_FILE, "{}: {}\n", timestamp, msg);
    write_res.expect("Failed writing to logs");
}

// Logging utility function, print out each message, for debugging
#[allow(dead_code)]
fn print_message(buf: &[u8; DATAGRAM_LENGTH]) {
    let mut curr: usize = 0;
    while (curr + 10) < DATAGRAM_LENGTH {
        let int_string = format!(
            "[{}]",
            buf[curr..curr + 10]
                .iter()
                .map(|&byte| byte.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        );
        log(int_string);
        curr += 10;
    }
    let int_string = format!(
        "[{}]",
        buf[curr..DATAGRAM_LENGTH]
            .iter()
            .map(|&byte| byte.to_string())
            .collect::<Vec<String>>()
            .join(", ")
    );
    log(int_string);
}

// Utility function to Modify/Toggle suspicion mode if cooldown fulfilled
fn toggle_suspicion_with_cooldown(mode: &Arc<Mutex<Mode>>, new_mode: u8) {
    let mode_lock = mode.lock().unwrap();
    let time_diff = SystemTime::now().duration_since(mode_lock.last_changed);
    drop(mode_lock);
    match time_diff {
        Ok(diff) => {
            if diff >= MODE_CHANGE_COOLDOWN {
                toggle_suspicion(&mode, new_mode);
                println!("Enter a command:"); // Without this, the print in toggle_suspicion messes with user command loop
            }
        }
        Err(_) => {}
    }
}

// Populate given buffer with enheartbeatNUM_OFFSET..heartbeatNUM_OFFSET + 2try using pre-defined protocol
fn populate_entry_bytes(buf: &mut [u8; DATAGRAM_LENGTH], entry: &MemListEntry, mode: u8) {
    // Populate hostname
    let length = entry.hostname.len(); // Note that this is not recommended, but it's okay as long as stay in ASCII
    buf[HOSTNAME_OFFSET..length].copy_from_slice(entry.hostname.as_bytes());
    buf[PORTNUM_OFFSET..PORTNUM_OFFSET + 2].copy_from_slice(&entry.port.to_le_bytes());
    buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8].copy_from_slice(&entry.timestamp.to_le_bytes());
    buf[HEARTBEAT_OFFSET..HEARTBEAT_OFFSET + 4].copy_from_slice(&entry.heartbeat.to_le_bytes());
    buf[STATUS_OFFSET..STATUS_OFFSET + 1].copy_from_slice(&entry.status.to_le_bytes());
    buf[INCNUM_OFFSET..INCNUM_OFFSET + 4].copy_from_slice(&entry.inc_num.to_le_bytes());
    buf[MODE_OFFSET..MODE_OFFSET + 1].copy_from_slice(&mode.to_le_bytes());
}

// Return MemListEntry and mode heartbeatNUM_OFFSET..heartbeatNUM_OFFSET + 2based on data from buffer
fn read_entry_bytes(buf: &[u8; DATAGRAM_LENGTH]) -> (u8, MemListEntry) {
    // Extract hostname
    let hostname_end = buf.iter().position(|&c| c == b'\0').unwrap();
    let hostname_bytes: Vec<u8> = Vec::from(&buf[HOSTNAME_OFFSET..HOSTNAME_OFFSET + hostname_end]);
    let hostname: String = String::from_utf8(hostname_bytes).unwrap();

    // Extract port number
    let mut port_bytes: [u8; 2] = [0; 2];
    port_bytes.copy_from_slice(&buf[PORTNUM_OFFSET..PORTNUM_OFFSET + 2]);
    let port: u16 = u16::from_le_bytes(port_bytes);

    // Extract timestamp
    let mut timestamp_bytes: [u8; 8] = [0; 8];
    timestamp_bytes.copy_from_slice(&buf[TIMESTAMP_OFFSET..TIMESTAMP_OFFSET + 8]);
    let timestamp: u64 = u64::from_le_bytes(timestamp_bytes);

    // Extract heartbeat counter
    let mut heartbeat_bytes: [u8; 4] = [0; 4];
    heartbeat_bytes.copy_from_slice(&buf[HEARTBEAT_OFFSET..HEARTBEAT_OFFSET + 4]);
    let heartbeat: u32 = u32::from_le_bytes(heartbeat_bytes);

    // Extract status
    let status: u8 = buf[STATUS_OFFSET];

    // Extract inc_num
    let mut inc_num_bytes: [u8; 4] = [0; 4];
    inc_num_bytes.copy_from_slice(&buf[INCNUM_OFFSET..INCNUM_OFFSET + 4]);
    let inc_num: u32 = u32::from_le_bytes(inc_num_bytes);

    // Extract mode
    let mode: u8 = buf[MODE_OFFSET];

    (
        mode,
        MemListEntry {
            hostname: hostname,
            port: port,
            timestamp: timestamp,
            heartbeat: heartbeat,
            local_time: SystemTime::now(),
            status: status,
            inc_num: inc_num,
        },
    )
}

// <------------------------ NETWORKING LOGIC FUNCTIONS ------------------------>

// Listens for UDP messages, updates membership list
fn update_membership(
    mem_list: Arc<Mutex<HashMap<String, MemListEntry>>>,
    mode: Arc<Mutex<Mode>>,
    self_id: String,
) {
    // Socket for UDP listening
    let socket_addr = String::from("0.0.0.0") + ":" + HEARTBEAT_PORT.to_string().as_str();
    let socket = UdpSocket::bind(socket_addr).unwrap();

    // Thread loop
    loop {
        // Gracefully terminate thread if peer has requested to leave
        {
            let mem_list = mem_list.lock().unwrap();
            let heartbeat = mem_list.get(&self_id).unwrap().heartbeat;
            if heartbeat == 0 {
                return;
            }
        }

        let mut buf: [u8; DATAGRAM_LENGTH * 10] = [0; DATAGRAM_LENGTH * 10];
        match socket.recv(&mut buf) {
            Ok(_) => {
                // Skip reading messages at rate of MESSAGE_DROP_RATE
                if rand::thread_rng().gen_range(0.0..1.0) < MESSAGE_DROP_RATE {
                    continue;
                }
            }
            Err(_) => {
                continue;
            }
        }

        for count in 0..10 {
            let start_offset = count * DATAGRAM_LENGTH;
            let buf_subset: [u8; DATAGRAM_LENGTH] = buf
                [start_offset..start_offset + DATAGRAM_LENGTH]
                .try_into()
                .unwrap();

            let (new_mode, entry) = read_entry_bytes(&buf_subset);

            // No more entries
            if entry.hostname.is_empty() {
                break;
            }

            // Check new mode and attempt to set mode if it's changed
            let mut curr_mode: u8;
            {
                let mode = mode.lock().unwrap();
                curr_mode = mode.mode;
            }
            if new_mode != curr_mode {
                toggle_suspicion_with_cooldown(&mode, new_mode);
                {
                    let mode = mode.lock().unwrap();
                    curr_mode = mode.mode;
                }
            }

            let id: String = generate_id(&entry.hostname, entry.port, entry.timestamp);
            log("Received entry: ".to_string() + id.as_str());

            let mut mem_list = mem_list.lock().unwrap();

            // Add new entry
            if !mem_list.contains_key(&id) {
                mem_list.insert(id.clone(), entry);
                log("Entry added: ".to_string() + &id);
            }
            // Entry already exists
            else {
                let curr_entry = mem_list.get_mut(&id).unwrap();

                // Non-suspicion mode
                if entry.heartbeat > curr_entry.heartbeat || entry.heartbeat == 0 {
                    curr_entry.heartbeat = entry.heartbeat;
                    curr_entry.local_time = SystemTime::now();
                    // if entry.heartbeat != 0 {
                    //     curr_entry.status = entry.status;
                    // }
                }
                // // Update failed entries received from other nodes
                // if entry.status == 1 {
                //     curr_entry.status = 1;
                // }

                // Suspicion mode
                if curr_mode == 1 {
                    // Correct any entries that say that self is suspected
                    if id == self_id && entry.status == 2 {
                        curr_entry.inc_num += 1;
                    }
                    // Update failed entries received from other node, regardless of inc num
                    else if entry.status == 1 {
                        curr_entry.status = 1;
                    }
                    // // Within inc num, suspected wins
                    // else if entry.inc_num == curr_entry.inc_num && entry.status == 2 {
                    //     curr_entry.status = 2;
                    //     // TODO: add a print statement here
                    // }
                    // // Higher inc num
                    // else if entry.inc_num > curr_entry.inc_num {
                    //     curr_entry.inc_num = entry.inc_num;
                    //     curr_entry.status = 0;
                    // }

                    // Update local status to suspected, only if inc num is greater and not currently failed
                    else if entry.inc_num == curr_entry.inc_num
                        && entry.status == 2
                        && curr_entry.status != 1
                    {
                        // Print to stdout only when newly changing suspicion status
                        if curr_entry.status != 2 {
                            let curr_time = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap();
                            let timestamp: u64 = curr_time.as_secs();
                            println!(
                                "Received and updated suspected at {} for {}",
                                timestamp, &entry.hostname
                            );
                        }
                        curr_entry.status = 2;

                        log("Received and updated suspected status for: ".to_string()
                            + &entry.hostname);
                    }
                    // Recover local suspected state is message inc num is greater
                    else if entry.inc_num > curr_entry.inc_num && curr_entry.status != 1 {
                        curr_entry.status = entry.status
                    }
                }
            }
        }
    }
}

// Continuously checks if members have failed
fn check_timeout(
    mem_list_arc: Arc<Mutex<HashMap<String, MemListEntry>>>,
    mode_arc: Arc<Mutex<Mode>>,
    self_id: String,
) {
    loop {
        let curr_mode: u8;
        {
            let mode = mode_arc.lock().unwrap();
            curr_mode = mode.mode;
        }
        let mut mem_list = mem_list_arc.lock().unwrap();

        // Gracefully terminate thread if peer has requested to leave
        let self_entry = mem_list.get_mut(&self_id).unwrap();
        if self_entry.heartbeat == 0 {
            return;
        }

        let now = SystemTime::now();
        let mut to_remove: Vec<String> = Vec::new();
        for (key, entry) in mem_list.iter_mut() {
            let time_diff = now.duration_since(entry.local_time);

            // Skip if it's self
            if key == &self_id {
                continue;
            }

            // Check as duration since might be negative due to system issues
            let time_diff = match time_diff {
                Ok(diff) => diff,
                Err(_) => {
                    continue;
                }
            };

            // Non-suspicion mode
            if curr_mode == 0 {
                // Failed, completed T_cleanup
                if entry.status == 1 && time_diff >= (TFAIL + TCLEANUP) {
                    to_remove.push(key.clone());
                    // TODO: remove in final submission
                    log("Going to delete entry: ".to_string()
                        + key
                        + "time_diff="
                        + &time_diff.as_secs().to_string());
                    println!(
                        "Going to delete entry: {}, time_diff= {}",
                        key,
                        &time_diff.as_secs().to_string()
                    );
                } else if entry.status == 0 && time_diff >= TFAIL {
                    entry.status = 1;
                    log("Entry failed, pending T_cleanup: ".to_string()
                        + key
                        + time_diff.as_secs().to_string().as_str());
                    println!("Failed: {}, Time diff: {:?}", key, time_diff); // TODO: remove in final submission
                } else if entry.heartbeat == 0 {
                    entry.status = 1;
                    log("Entry left, pending T_cleanup: ".to_string() + key);
                }
            }
            // Suspicion mode
            else if curr_mode == 1 {
                // Timed out, suspect it
                if entry.status == 0 && time_diff >= TFAIL {
                    entry.status = 2;
                    log("Entry timed out, now suspected: ".to_string() + key);

                    let curr_time = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap();
                    let timestamp: u64 = curr_time.as_secs();
                    println!("Entry timed out at {}, now suspected: {}", timestamp, key);
                    println!("Enter a command:"); // Make sure to continue user prompt when printing to stdout
                }
                // Alrd suspected, timed out, now failed
                else if entry.status == 2 && time_diff >= (TFAIL + TSUSTIMEOUT) {
                    entry.status = 1;
                    log("Entry failed, pending T_cleanup: ".to_string() + key);
                }
                // Failed, completed T_cleanup
                else if entry.status == 1 && time_diff >= (TFAIL + TSUSTIMEOUT + TCLEANUP) {
                    to_remove.push(key.clone());

                    // TODO: remove in final submission
                    log("Going to delete entry: ".to_string()
                        + key
                        + "time_diff="
                        + &time_diff.as_secs().to_string());
                    println!(
                        "Going to delete entry: {}, time_diff= {}",
                        key,
                        &time_diff.as_secs().to_string()
                    );
                }
                // Entry gave leave command
                else if entry.heartbeat == 0 {
                    entry.status = 1;
                    log("Entry left, pending T_cleanup: ".to_string() + key);
                }
            }
        }

        for item in to_remove.iter() {
            mem_list.remove(item);
            log("Deleted entry: ".to_string() + item);
        }
        drop(mem_list);
        thread::sleep(TFAIL);
    }
}

// WARNING: CONCURRENCY NOT ENFORCED, ENSURE CALLING THREAD HAS LOCK OVER MEM_LIST
// Gossips membership list to specified peer
fn send_mem_list(
    dest_hostname: &str,
    dest_port: u16,
    mem_list: &HashMap<String, MemListEntry>,
    mode: u8,
) {
    let dest_socket_addr: String =
        String::from(dest_hostname) + ":" + dest_port.to_string().as_str();
    // Let OS assign a port
    let socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(socket) => socket,
        Err(_) => return,
    };
    match socket.connect(dest_socket_addr) {
        Ok(_) => {}
        Err(_) => return,
    };

    let mut buf: [u8; 10 * DATAGRAM_LENGTH] = [0; 10 * DATAGRAM_LENGTH];
    let mut count: usize = 0;
    for (_key, entry) in mem_list.iter() {
        // Do not send failed nodes
        if entry.status == 1 {
            continue;
        }

        let start_offset = count * DATAGRAM_LENGTH;
        let mut buf_subset: [u8; DATAGRAM_LENGTH] = [0; DATAGRAM_LENGTH];
        populate_entry_bytes(&mut buf_subset, entry, mode);
        buf[start_offset..start_offset + DATAGRAM_LENGTH].copy_from_slice(&buf_subset);
        count += 1;
    }

    // Send whole buffer
    match socket.send(&buf) {
        Ok(_) => {
            log("Gossipped to: ".to_string()
                + dest_hostname
                + ":"
                + dest_port.to_string().as_str());
        }
        Err(_) => {
            log("Send to failed : ".to_string()
                + dest_hostname
                + ":"
                + dest_port.to_string().as_str());
        }
    }
}

// Gossips membership list to random subset of peers on set interval
fn gossip(
    mem_list_arc: Arc<Mutex<HashMap<String, MemListEntry>>>,
    mode: Arc<Mutex<Mode>>,
    self_id: String,
) {
    loop {
        let curr_mode: u8;
        {
            curr_mode = mode.lock().unwrap().mode;
        }

        let mut mem_list = mem_list_arc.lock().unwrap();

        // Increase self heartbeat counter
        let self_entry = mem_list.get_mut(&self_id).unwrap();
        self_entry.heartbeat += 1;

        // For case where membership list is smaller than desired
        if mem_list.len() - 1 <= GOSSIP_NUM {
            for (key, entry) in mem_list.iter() {
                // Skip sending to self
                if key == &self_id {
                    continue;
                }
                send_mem_list(&entry.hostname, entry.port, &mem_list, curr_mode);
            }
        }
        // Case where membership list is larger and we can choose
        else {
            // Create random subset of membership list of length GOSSIP_NUM
            let mut to_gossip_to: Vec<String> = Vec::new();

            let mut rng = rand::thread_rng();
            let mut keys: Vec<_> = mem_list.keys().collect();
            while to_gossip_to.len() < GOSSIP_NUM {
                let random_index = rng.gen_range(0..keys.len());
                let random_key = keys[random_index];
                to_gossip_to.push(random_key.to_string());
                keys.remove(random_index);
            }

            for to_gossip_key in to_gossip_to.iter() {
                let peer = &mem_list[to_gossip_key];
                send_mem_list(&peer.hostname, peer.port, &mem_list, curr_mode);
            }
        }

        // Gracefully leave after gossipping
        let heartbeat = mem_list.get(&self_id).unwrap().heartbeat;
        if heartbeat == 0 {
            return;
        }

        drop(mem_list);

        thread::sleep(TGOSSIP);
    }
}
