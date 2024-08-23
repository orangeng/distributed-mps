extern crate log;
use std::io::{self, Error, ErrorKind, Read, Write};
use std::net::TcpStream;

use log::info;
use mj::*;

fn main() {
    // <----------------- SET UP LOGGER----------------- >
    setup_logger();

    // TODO: start up thread to listen for updates from Leader

    loop {
        println!("[MapleJuice] Enter your command:");

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");

        let input = input.trim();

        if input.to_lowercase() == "exit" {
            println!("Goodbye!");
            break;
        }

        let arguments: Vec<&str> = input.split_whitespace().collect();

        if arguments[0] == "maple" && arguments.len() >= 5 {
            // Expected structure for custome MJ command, maple step:
            // maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_filename> [<custom_params>]
            let num_maples = match arguments[2].parse::<u8>() {
                Ok(num) => num,
                Err(_) => 0,
            };

            if num_maples == 0 {
                println!("Please enter a valid num_maples value");
            } else {
                println!("[MapleJuice] Running Maple command...");

                let custom_params: Vec<&str> = arguments[5..].into();

                println!(
                    "[MapleJuice] {}",
                    start_maple_command(
                        arguments[1],
                        num_maples,
                        arguments[3],
                        arguments[4],
                        custom_params
                    )
                );
            }
        } else if arguments[0] == "juice" && arguments.len() == 6 {
            // Expected structure for custome MJ command, juice step:
            // juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}
            let num_juices = match arguments[2].parse::<u8>() {
                Ok(num) => num,
                Err(_) => 0,
            };

            let delete_input = match arguments[5].parse::<u8>() {
                Ok(num) => num,
                Err(_) => 0,
            };

            if num_juices == 0 {
                println!("Please enter a valid num_juices value");
            } else {
                println!("[MapleJuice] Running Juice command...");

                println!(
                    "[MapleJuice] {}",
                    start_juice_command(
                        arguments[1],
                        num_juices,
                        arguments[3],
                        arguments[4],
                        delete_input
                    )
                );
            }
        } else if arguments[0] == "sql" && arguments.len() >= 2 {
            // Expected structure for sql command:
            // sql SELECT ALL FROM sdfs_src_filename WHERE <regex> INTO sdfs_dest_filename IN <num_tasks> TASKS
            let num_tasks = match arguments[10].parse::<u8>() {
                Ok(num) => num,
                Err(_) => 0,
            };

            println!("[MapleJuice] Running SQL command...");
            println!(
                "[MapleJuice] {}",
                start_sql_command(arguments[4], arguments[6], arguments[8], num_tasks)
            )
        } else {
            println!("Unrecognized command, please try again.");
        }
    }
}

// <----------------- CLIENT FUNCTIONS ----------------- >
// function to start the maple functionality. takes the user args, send request to leader, listen for and returns status received from leader (async functions? channels? either way block main thread/don't allow more commonds somehow)
fn start_maple_command(
    maple_exe: &str,
    num_maples: u8,
    inter_file_prefix: &str,
    sdfs_src_file: &str,
    custom_params: Vec<&str>,
) -> String {
    match send_command_request(
        MAPLE_TYPE_ID,
        num_maples,
        maple_exe,
        inter_file_prefix,
        sdfs_src_file,
        0,
        custom_params,
    ) {
        Ok(_) => {
            info!("Successfully sent maple command request to leader.");

            return "Successfully sent maple command request to leader. Please wait until confirmation of job completion before running next command.".to_string();
            // TODO: eventually block the thread instead
        }
        Err(err) => {
            return "Error in sending maple command: ".to_string() + &err.to_string();
        }
    }
}

// function to start the juice functionality. takes the user args, send request to leader, listen for and returns status received from leader (async functions? channels? either way block main thread/don't allow more commonds somehow)
fn start_juice_command(
    juice_exe: &str,
    num_juice: u8,
    inter_file_prefix: &str,
    sdfs_dest_file: &str,
    delete_input: u8,
) -> String {
    let empty: Vec<&str> = Vec::new();
    match send_command_request(
        JUICE_TYPE_ID,
        num_juice,
        juice_exe,
        inter_file_prefix,
        sdfs_dest_file,
        delete_input,
        empty,
    ) {
        Ok(_) => {
            info!("Successfully sent juice command request to leader.");

            return "Successfully sent juice command request to leader. Please wait until confirmation of job completion before running next command.".to_string();
            // TODO: eventually block the thread instead
        }
        Err(err) => {
            return "Error in sending juice command: ".to_string() + &err.to_string();
        }
    }
}

// function to start the sql command functionality. takes the user args, send request to leader, listen for and returns status received from leader (async functions? channels? either way block main thread/don't allow more commonds somehow)
fn start_sql_command(
    sdfs_src_file: &str,
    regex: &str,
    sdfs_dest_file: &str,
    num_tasks: u8,
) -> String {
    match send_command_request(
        MAPLE_TYPE_ID,
        num_tasks,
        SQL_FILE_EXE,
        sdfs_dest_file,
        sdfs_src_file,
        0,
        vec![regex],
    ) {
        Ok(_) => {
            info!("Successfully sent SQL command request to leader.");

            return "Successfully sent SQL command request to leader. Please wait until confirmation of job completion before running next command.".to_string();
            // TODO: eventually block the thread instead
        }
        Err(err) => {
            return "Error in sending SQL command: ".to_string() + &err.to_string();
        }
    }
}

// Generalized function to send command request to leader
fn send_command_request(
    command_type: u8,
    num_tasks: u8,
    target_exe: &str,
    inter_file_prefix: &str,
    target_dest: &str,
    delete_input: u8,
    custom_params: Vec<&str>,
) -> Result<(), Error> {
    // Connect to master
    let sock_addr = String::from(LEADER_VM_ID) + ":" + CL_PORT;
    let mut stream = TcpStream::connect(sock_addr)?;

    // Write command request to stream
    let to_send: [u8; 3] = [command_type, num_tasks, delete_input];
    stream.write(&to_send).unwrap();

    write_str_to_stream(target_exe, &mut stream)?;
    write_str_to_stream(inter_file_prefix, &mut stream)?;
    write_str_to_stream(target_dest, &mut stream)?;
    if let Err(err) = write_custom_params(custom_params, &mut stream) {
        return Err(Error::new(
            ErrorKind::Other,
            "Error in sending command request to leader: ".to_string() + &err.to_string(),
        ));
    }

    // Read reply from master
    let mut reply_status: [u8; 1] = [0];
    stream.read_exact(&mut reply_status).unwrap();
    if reply_status[0] == SUCCESS_STATUS_CODE {
        return Ok(());
    } else {
        return Err(Error::new(
            ErrorKind::Other,
            "Leader did not accept command request".to_string(),
        ));
    }
}
