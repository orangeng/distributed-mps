use std::fs::File;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use logger::*;

fn main() {
    // Initialise port and addresses
    let sock_addr: String = String::from("0.0.0.0") + ":" + PORT;
    println!("Listening on: {}", sock_addr);

    // Create TCP socket listener
    let listener = TcpListener::bind(sock_addr).unwrap(); //TODO: handle error

    // Spawn heartbeat listener
    thread::spawn(|| {
        let sock_addr: String = String::from("0.0.0.0") + ":" + HEARTBEAT_PORT;
        let heartbeat_listen = TcpListener::bind(sock_addr).unwrap();
        for _ in heartbeat_listen.incoming() {
            // Accepts and do nothing
        }
    });

    // Spawn heartbeat query-er
    thread::spawn(|| loop {
        let mut status: [u8; 10] = [0; 10];
        for (idx, host) in HOSTS.iter().enumerate() {
            let sock_addr: String = (*host).to_owned() + ":" + HEARTBEAT_PORT;
            let addrs: Vec<_> = sock_addr.to_socket_addrs().unwrap().collect();
            match TcpStream::connect_timeout(&addrs[0], Duration::new(1, 0)) {
                Ok(_) => {
                    status[idx] = 1;
                }
                Err(_) => {
                    status[idx] = 0;
                }
            }
        }
        let mut f = File::create(HEARTBEAT_FILE).unwrap();
        f.write_all(&status).unwrap();
        thread::sleep(Duration::new(1, 0));
    });

    // Equivalent to a while loop that keeps waiting for and accepting incoming TCP connections
    for stream_rs in listener.incoming() {
        match stream_rs {
            Ok(mut stream) => {
                handle_connection(&mut stream);
            }
            Err(e) => {
                println!("Error: {}", e.to_string());
            }
        }
    }
}

fn handle_connection(stream: &mut TcpStream) {
    println!("");

    // Read command size
    let command_size: i32 = payload_size(stream);

    // Read command
    let mut command_buf: Vec<u8> = vec![0; command_size as usize];
    stream.read_exact(&mut command_buf).unwrap(); // TODO: handle error
    let mut command_str: String = String::from_utf8(command_buf).unwrap(); // TODO: Handle
    trim_newline(&mut command_str);
    println!("Command: [{}]", command_str); // TODO: comment for final build

    // Build command
    let mut args: Vec<&str> = command_str.split(DELIM).collect();
    let mut command = Command::new(args.get(0).unwrap());
    args.remove(0);
    command.args(args);
    command.stdout(Stdio::piped());

    let proc = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            println!("Failed to spawn child for command '{}'", command_str);
            println!("{}", e.to_string());
            stream.shutdown(Shutdown::Both).unwrap(); // TODO: Handle
            return;
        }
    };

    let mut child_stdout = proc.stdout.unwrap(); // TODO: Handle

    // Read from child process's stdout
    let mut buf: [u8; 512] = [0; 512];
    while let Ok(bytes_read) = child_stdout.read(&mut buf) {
        // Nothing left to write
        if bytes_read == 0 {
            let zero_byte = i32::to_le_bytes(0);
            stream.write(&zero_byte).unwrap(); //TODO: Handle
            break;
        }

        // Writing
        let size_bytes = i32::to_le_bytes(bytes_read as i32);
        stream.write(&size_bytes).unwrap(); // TODO: Handle
        stream.write(&buf[0..bytes_read]).unwrap(); // TODO: Handle
        stream.flush().unwrap(); // TODO: Handle
    }

    // Handle cleanup
    stream.flush().unwrap(); // TODO: Handle
    stream.shutdown(Shutdown::Both).unwrap(); //TODO: Handle

    println!("Ready for next connection...");
}
