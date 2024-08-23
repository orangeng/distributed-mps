use std::env;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::TcpStream;

extern crate utils;

fn main() {
    // TODO: check why args need to be entered immediately/can we print instructions like real cli? (low imp)
    // TODO: handle grep command with no arg like "-i"

    // Get arguments passed into program
    let mut argv: Vec<String> = env::args().collect();
    argv.remove(0);
    if argv.is_empty() {
        println!("No patterns specified!");
        return;
    }
    argv.insert(0, String::from("grep"));

    // Build command for grep -c
    let mut argv_c = argv.clone();
    argv_c.insert(1, String::from("-c"));

    // Count of total lines
    let mut total_count: i32 = 0;

    // Loop through the list of hosts
    for (idx, host) in utils::HOSTS.iter().enumerate() {

        println!("");

        // Read file to see if hosts is up
        let mut f = File::open(utils::HEARTBEAT_FILE).unwrap();
        let mut status: [u8; 10] = [0; 10];
        f.read_exact(&mut status).unwrap();
        // Host is down, skip
        if status[idx] == 0{
            println!("VM #{}: [Machine down]", idx + 1);
            continue;
        }

        let vm_num = idx + 1; // VMs are 1-indexed
        let file_name = format! {"vm{}.log", vm_num};

        // Build command
        let command: String = build_command(&argv, &file_name);

        // Define address
        let dest_addr: String = (*host).to_owned() + ":" + utils::PORT;

        // Establish connection
        let mut stream: TcpStream;
        match TcpStream::connect(&dest_addr) {
            Ok(s) => {
                println!("VM #{}: [Online]", vm_num);
                stream = s;
            }
            Err(e) => {
                println!("VM #{}:[Down ({})]", vm_num, e); //TODO: Handle
                continue;
            }
        }

        // Send command
        send_command(&mut stream, command);

        // Read back grep from server
        loop {
            // Read size bytes
            let size: i32 = utils::payload_size(&mut stream);
            if size == 0 {
                break;
            }

            // Read contents
            let mut buf: Vec<u8> = vec![0; size as usize];
            stream.read_exact(&mut buf).unwrap(); //TODO: Handle
            io::stdout().write(&buf).unwrap(); //TODO: Handle
            io::stdout().flush().unwrap(); //TODO: Handle
        }

        // Build count command
        let command: String = build_command(&argv_c, &file_name);

        // Send command
        let mut stream = TcpStream::connect(&dest_addr).unwrap(); // Should be up by this point
        send_command(&mut stream, command);

        // Read back grep -c from server
        let mut count_bytes: Vec<u8> = Vec::new();
        loop {
            // Read size bytes
            let size: i32 = utils::payload_size(&mut stream);
            if size == 0 {
                break;
            }

            // Read contents
            let mut buf: Vec<u8> = vec![0; size as usize];
            stream.read_exact(&mut buf).unwrap(); //TODO: Handle
            count_bytes.append(&mut buf);
        }

        // Process count
        let mut count_string = String::from_utf8(count_bytes).unwrap(); // TODO: Handle lmao (errors like file not found in server break here)
        utils::trim_newline(&mut count_string);
        let count: i32 = count_string.parse().unwrap();
        total_count += count;
        println!("Count: {}", count);
    }

    println!("\nTotal count: {}", total_count);
    println!("Done reading all VM logs.")
}

fn build_command(argv: &Vec<String>, file_name: &String) -> String {
    let mut command: String = String::new();
    for i in 0..argv.len() {
        command += argv.get(i).unwrap();
        command += utils::DELIM;
    }
    command += file_name;
    command
}

// Sends command string over stream, with leading 4 size bytes
fn send_command(stream: &mut TcpStream, command: String) {
    let command_bytes = command.into_bytes();
    let size_bytes = i32::to_le_bytes(command_bytes.len() as i32);

    stream.write(&size_bytes).unwrap(); // TODO: handle
    stream.write(&command_bytes).unwrap(); //TODO: handle
}

#[cfg(test)]
#[path = "../tests.rs"]
mod tests;
