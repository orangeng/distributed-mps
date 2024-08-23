use std::fs::OpenOptions;

use crate::*;

// Copy file from SDFS to local FILEPATH (even if it already exists, to update local copy)
pub fn get_file(master: u8, sdfsfilename: &str, localfilename: &str) -> Result<(), String> {
    let to_read_datanode_res = send_read_request(master, 0, sdfsfilename);
    if let Err(err) = to_read_datanode_res {
        return Err("Error in sending request to get file: ".to_string() + &err);
    }

    let to_read_datanode = to_read_datanode_res.unwrap();
    if to_read_datanode == 0 {
        return Err("Error in getting file: File not found in SDFS".to_string());
    }

    match get_from_datanode(sdfsfilename, localfilename, &to_read_datanode) {
        Ok(_) => {
            return Ok(());
        }
        Err(err) => {
            return Err("Error in getting file: ".to_string() + &err);
        }
    }
}

fn send_read_request(master: u8, client_id: u8, remote_filename: &str) -> Result<u8, String> {
    // Connect to master
    let sock_addr = String::from(VM_LIST[master as usize]) + ":" + CM_PORT;
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

fn get_from_datanode(
    remote_filename: &str,
    local_filename: &str,
    to_read_datanode: &u8,
) -> Result<(), String> {
    // Open file, or create it if it doesn't exist
    let file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(local_filename)
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
    Ok(())
}

pub fn put_file(master: u8, localfilename: &str, sdfsfilename: &str) -> Result<(), String> {
    let free_datanodes_res = send_write_request(master, 0, 4, sdfsfilename);
    if let Err(err) = free_datanodes_res {
        return Err("Error in receiving request to put file: ".to_string() + &err);
    }
    let free_datanodes = free_datanodes_res.unwrap();

    match write_to_datanode(localfilename, sdfsfilename, free_datanodes) {
        Ok(_) => {
            return Ok(());
        }
        Err(err) => {
            return Err("Error in putting file: ".to_string() + &err);
        }
    }
}

fn send_write_request(
    master: u8,
    client_id: u8,
    no_datanodes: u8,
    remote_filename: &str,
) -> Result<Vec<u8>, String> {
    // Connect to master
    let sock_addr = String::from(VM_LIST[master as usize]) + ":" + CM_PORT;
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
