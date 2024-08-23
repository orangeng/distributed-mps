extern crate chrono;

use chrono::prelude::*;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;

use mp3::*;

fn main() {
    // Open file to read client ID
    let mut id_buf = String::new();
    let mut id_file = File::open(VM_ID_PATH).unwrap();
    let _ = id_file.read_to_string(&mut id_buf);
    let client_id: u8 = id_buf.trim().parse().unwrap();

    let master = get_master();

    loop {
        println!("[SDFS] Enter your command:");

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        let input = input.trim().to_lowercase();

        if input.to_lowercase() == "exit" {
            println!("Goodbye!");
            break;
        }

        let arguments: Vec<&str> = input.split_whitespace().collect();

        if arguments[0] == "put" && arguments.len() == 3 {
            println!(
                "[SDFS] {}",
                put_file(master, client_id, arguments[1], arguments[2])
            )
        } else if arguments[0] == "get" && arguments.len() == 3 {
            println!(
                "[SDFS] {}",
                get_file(master, client_id, arguments[1], arguments[2])
            )
        } else if arguments[0] == "ls" && arguments.len() == 2 {
            list_file(master, arguments[1]);
        } else if arguments[0] == "store" && arguments.len() == 1 {
            list_local_store();
        } else {
            println!("Unrecognized command, please try again.");
        }
    }
}

// <----------- Main command functions: ----------->
fn put_file(master: u8, client_id: u8, localfilename: &str, sdfsfilename: &str) -> String {
    let free_datanodes_res = send_write_request(master, client_id, 4, sdfsfilename);
    if let Err(err) = free_datanodes_res {
        return "Error in receiving request to put file: ".to_string() + &err;
    }
    let free_datanodes = free_datanodes_res.unwrap();

    println!("Datanodes assigned: {:?}", free_datanodes);

    match write_to_datanode(localfilename, sdfsfilename, free_datanodes) {
        Ok(output) => {
            println!("Datanodes written: {:?}", output);
            return "Successfully put file".to_string();
        }
        Err(err) => {
            return "Error in putting file: ".to_string() + &err;
        }
    }
}

fn get_file(master: u8, client_id: u8, sdfsfilename: &str, localfilename: &str) -> String {
    let to_read_datanode_res = send_read_request(master, client_id, sdfsfilename);
    if let Err(err) = to_read_datanode_res {
        return "Error in receiving request to get file: ".to_string() + &err;
    }
    let to_read_datanode = to_read_datanode_res.unwrap();
    if to_read_datanode == 0 {
        return "Error in getting file: file not found in SDFS".to_string();
    }
    println!("Datanode assigned to read from: {:?}", to_read_datanode);

    match get_from_datanode(sdfsfilename, localfilename, &to_read_datanode) {
        Ok(output) => {
            println!("Datanodes read from: {:?}", output);
            return output;
        }
        Err(err) => {
            return "Error in getting file: ".to_string() + &err;
        }
    }
}

fn list_file(master: u8, sdfsfilename: &str) {
    match send_ls_request(master, sdfsfilename) {
        Ok(list) => {
            println!("{} is stored on {:?}", sdfsfilename, list);
        }
        Err(err) => {
            println!("Failed to find {}, error: {}", sdfsfilename, err)
        }
    }
}

fn list_local_store() {
    if let Ok(entries) = fs::read_dir(FILES_PATH) {
        println!("Locally stored files: ");
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                    if let Some(file_name) = entry.file_name().to_str() {
                        println!("    >{}", file_name);
                    }
                }
            }
        }
    } else {
        println!("Error reading directory");
    }
}

// <----------- Utility functions: ----------->

// Query the first available datanode for the current master
// Note that the servers are numbered [1 - 10]!
fn get_master() -> u8 {
    let membership = get_membership();
    for i in 0..membership.len() {
        if membership[i] != 1 {
            continue;
        }

        let sock_addr = String::from(VM_LIST[i]) + ":" + CD_PORT;
        if let Ok(mut stream) = TcpStream::connect(sock_addr) {
            stream.write(&[1]).unwrap();
            let mut master: [u8; 1] = [0];
            stream.read_exact(&mut master).unwrap();
            return master[0];
        }
    }

    return 0;
}

fn send_write_request(
    master: u8,
    client_id: u8,
    no_datanodes: u8,
    remote_filename: &str,
) -> Result<Vec<u8>, String> {
    // Connect to master
    let sock_addr = String::from(VM_LIST[(master - 1) as usize]) + ":" + CM_PORT;
    let mut stream = match TcpStream::connect(sock_addr) {
        Ok(stream) => stream,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    // Write put request to stream
    let to_send: [u8; 3] = [CM_PUT_REQ, client_id, no_datanodes];
    stream.write(&to_send).unwrap();

    let mut filename_bytes = String::from(remote_filename).into_bytes();
    filename_bytes.insert(0, filename_bytes.len() as u8);
    stream.write(&filename_bytes).unwrap();

    // Read reply from master
    let mut num_given: [u8; 1] = [0];
    stream.read_exact(&mut num_given).unwrap();
    let mut buf: Vec<u8> = vec![0; num_given[0] as usize];
    stream.read_exact(&mut buf).unwrap();
    return Ok(buf);
}

fn send_read_request(master: u8, client_id: u8, remote_filename: &str) -> Result<u8, String> {
    // Connect to master
    let sock_addr = String::from(VM_LIST[(master - 1) as usize]) + ":" + CM_PORT;
    let mut stream = match TcpStream::connect(sock_addr) {
        Ok(stream) => stream,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    // Write get request to stream
    let to_send: [u8; 2] = [CM_GET_REQ, client_id];
    stream.write(&to_send).unwrap();

    // Write filename to master
    stream
        .write(&generate_filename_bytes(&remote_filename))
        .unwrap();

    // Read reply from master
    let mut buf: Vec<u8> = vec![0; 1];
    stream.read_exact(&mut buf).unwrap();
    return Ok(buf[0]);
}

fn send_ls_request(master: u8, remote_filename: &str) -> Result<Vec<u8>, String> {
    // Connect to master
    let sock_addr = String::from(VM_LIST[(master - 1) as usize]) + ":" + CM_PORT;
    let mut stream = match TcpStream::connect(sock_addr) {
        Ok(stream) => stream,
        Err(e) => {
            return Err(e.to_string());
        }
    };
    // Write get request to stream
    let to_send: [u8; 1] = [CM_LS_REQ];
    if let Err(err) = stream.write(&to_send) {
        return Err(err.to_string());
    };

    // Write filename to master
    if let Err(err) = stream.write(&generate_filename_bytes(&remote_filename)) {
        return Err(err.to_string());
    };

    // Read reply from master
    let mut num_given: [u8; 1] = [0];
    if let Err(err) = stream.read_exact(&mut num_given) {
        return Err(err.to_string());
    };
    let mut buf: Vec<u8> = vec![0; num_given[0] as usize];
    if let Err(err) = stream.read_exact(&mut buf) {
        return Err(err.to_string());
    };
    return Ok(buf);
}

// Writes to set of datanodes. Returns the vector of the successful writes
fn write_to_datanode(
    local_filename: &str,
    remote_filename: &str,
    datanodes: Vec<u8>,
) -> Result<Vec<u8>, String> {
    // Open streams and write filename
    let mut streams: Vec<(u8, TcpStream)> = Vec::new();
    for node in datanodes {
        let sock_addr = String::from(VM_LIST[(node - 1) as usize]) + ":" + CD_PORT;
        if let Ok(mut stream) = TcpStream::connect(sock_addr) {
            stream.write(&[CD_WRITE_FILE]).unwrap();
            stream
                .write(&generate_filename_bytes(remote_filename))
                .unwrap();
            streams.push((node, stream));
        }
    }

    // Actually write to file (writes to all streams)
    // Vec of index of streams to remove (just-in-case)
    let mut to_remove: Vec<usize> = Vec::new();

    for i in 0..streams.len() {
        let (_, stream) = &mut streams[i];
        // Open file
        let file = match File::open(local_filename.clone()) {
            Ok(file) => file,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        if let Err(_e) = send_file_over_stream(stream, file) {
            to_remove.push(i);
        }
    }
    // Remove if connection fails
    for index in to_remove.iter().rev() {
        streams.remove(*index);
    }

    // Checks streams for 0xDEADBEEF confirmations
    let mut output: Vec<u8> = Vec::new();
    for (node, stream) in streams.iter_mut() {
        let mut confirmation: [u8; 4] = [0; 4];
        if let Ok(_) = stream.read(&mut confirmation) {
            if confirmation == CONFIRMATION {
                output.push(*node);
            }
        }
    }
    Ok(output)
}

fn get_from_datanode(
    remote_filename: &str,
    local_filename: &str,
    to_read_datanode: &u8,
) -> Result<String, String> {
    // Open file, or create it if it doesn't exist
    let file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(local_filename.clone())
    {
        Ok(file) => file,
        Err(e) => {
            return Err(e.to_string());
        }
    };

    let sock_addr = String::from(VM_LIST[(to_read_datanode - 1) as usize]) + ":" + CD_PORT;
    let stream_res = TcpStream::connect(sock_addr);
    if let Err(err) = stream_res {
        return Err(err.to_string());
    }
    let mut stream = stream_res.unwrap();
    stream.write(&[CD_READ_FILE]).unwrap();
    let res = stream.write(&generate_filename_bytes(remote_filename));
    if let Err(err) = res {
        return Err(err.to_string());
    }

    // Actually read from datanode into local_filename
    if let Err(err) = read_file_from_stream(&mut stream, file) {
        return Err(err.to_string());
    }
    Ok("Successfully got file".to_string())
}
