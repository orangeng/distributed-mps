use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::thread;

use log::info;
use mj::*;
use sdfs::{get_file, put_file};

fn main() {
    let leader_listener = thread::spawn(move || leader_listen());

    leader_listener.join().unwrap();
}

fn leader_listen() {
    let listen_addr = String::from("0:") + LW_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut request_type: [u8; 1] = [0];
            if stream.read_exact(&mut request_type).is_err() {
                continue;
            };

            match request_type[0] {
                // Maple Command Task
                MAPLE_TYPE_ID => {
                    thread::spawn(move || handle_maple_req(&mut stream));
                }
                // Juice Command Task
                JUICE_TYPE_ID => {
                    thread::spawn(move || handle_juice_req(&mut stream));
                }
                _ => {}
            }
        } else {
            continue;
        }
    }
}

fn handle_maple_req(mut stream: &mut TcpStream) {
    let range_start = read_i32_from_stream(&mut stream).unwrap();
    let range_end = read_i32_from_stream(&mut stream).unwrap();
    let maple_exe = read_str_from_stream(&mut stream).unwrap();
    let inter_file = read_str_from_stream(&mut stream).unwrap();
    let sdfs_file = read_str_from_stream(&mut stream).unwrap();
    let custom_params = read_custom_params(&mut stream).unwrap();

    println!(
        "{}, {}, {}, {}, {}, {:?}",
        range_start, range_end, maple_exe, inter_file, sdfs_file, custom_params
    );

    // Get data file + executable
    let localfile_path = String::from(MJ_FILES) + &sdfs_file;
    get_file(LEADER_VM_IDX, &sdfs_file, &localfile_path).unwrap();
    let localfile = File::open(localfile_path).unwrap();
    let buf_read = BufReader::new(localfile);
    let local_exe_path = String::from(MJ_FILES) + &maple_exe;
    get_file(LEADER_VM_IDX, &maple_exe, &local_exe_path).unwrap();

    // Command to exec each time
    let mut command = get_command(
        local_exe_path,
        WORKER_TEMP_INPUT,
        WORKER_TEMP_OUTPUT,
        custom_params,
    );

    // Loop through lines that this worker is responsible for
    let mut temp_input = File::create(WORKER_TEMP_INPUT).unwrap();
    let mut count = 0;
    let mut curr_line = range_start;
    for line in buf_read.lines().skip(range_start as usize) {
        let line = line.unwrap() + "\n";
        temp_input.write_all(line.as_bytes()).unwrap();
        count += 1;

        if count == MAPLE_FUNC_INPUT_SIZE {
            // Call worker
            command.output().unwrap();

            temp_input = File::create(WORKER_TEMP_INPUT).unwrap();
            count = 0;
        }

        if curr_line == range_end {
            break;
        }
        curr_line += 1;
    }
    command.output().unwrap();

    // Sort the output
    Command::new("sort")
        .arg("-o")
        .arg(WORKER_TEMP_OUTPUT_SORTED)
        .arg(WORKER_TEMP_OUTPUT)
        .output()
        .unwrap();

    // Write the output to sdfs - 1 file per key
    let output = File::open(WORKER_TEMP_OUTPUT_SORTED).unwrap();
    let buf_read = BufReader::new(output);

    let mut curr_key: String = String::new();
    let worker_id = get_vm_id();
    let keyfile_header = inter_file.clone() + "_" + worker_id.to_string().as_str() + "_";
    let mut curr_key_file: File =
        File::create(String::from(MJ_FILES) + keyfile_header.as_str() + curr_key.as_str()).unwrap();

    // Write the output to sdfs - 1 file per key
    // output_files is (key, filename)
    let mut output_files: Vec<(String, String)> = Vec::new();
    for line in buf_read.lines() {
        let line = line.unwrap();

        let items: Vec<&str> = line.split(",").collect();
        let (key, _) = (items[0].trim(), items[1].trim());
        let key = replace_invalid_chars(key);

        if key != curr_key {
            curr_key = String::from(key.clone());

            curr_key_file.flush().unwrap();
            let new_keyfile_name = keyfile_header.clone() + key.as_str();
            output_files.push((key, new_keyfile_name.clone()));
            curr_key_file =
                File::create(String::from(MJ_FILES) + new_keyfile_name.as_str()).unwrap();
        }

        curr_key_file.write((line + "\n").as_bytes()).unwrap();
    }
    println!("Key files: {:?}", output_files);

    for (_, filename) in output_files.iter() {
        put_file(
            LEADER_VM_IDX,
            &(String::from(MJ_FILES) + filename.as_str()),
            &filename,
        )
        .unwrap();
    }

    // Construct reply to leader
    let leader_addr = String::from(LEADER_VM_ID) + ":" + WL_PORT;
    let mut stream = TcpStream::connect(leader_addr).unwrap();
    let buf = [WL_MAPLE_DONE];
    stream.write(&buf).unwrap();

    write_u8_to_stream(worker_id, &mut stream).unwrap();
    write_str_to_stream(&inter_file, &mut stream).unwrap();
    write_key_filename_tuple(&mut stream, &output_files);

    // Remove intermediary files
    let mut to_remove = vec![
        WORKER_TEMP_INPUT,
        WORKER_TEMP_OUTPUT,
        WORKER_TEMP_OUTPUT_SORTED,
    ];
    output_files.iter_mut().for_each(|(_, filename)| {
        *filename = String::from(MJ_FILES) + filename.as_str();
    });
    let mut keyfiles_to_remove: Vec<&str> = output_files
        .iter()
        .map(|(_, filename)| filename.as_str())
        .collect();
    to_remove.append(&mut keyfiles_to_remove);
    println!("to_remove: {:?}", to_remove);
    remove_maple_temp_files(to_remove);
}

fn handle_juice_req(mut stream: &mut TcpStream) {
    let key_filenames_str = read_str_from_stream(&mut stream).unwrap();
    let juice_exe = read_str_from_stream(&mut stream).unwrap();
    let inter_file = read_str_from_stream(&mut stream).unwrap();
    let output_filename = read_str_from_stream(&mut stream).unwrap();

    let key_filenames: Vec<&str> = key_filenames_str.split(',').collect();

    info!(
        "Worker: received juice request: {:?}, {}, {}",
        key_filenames, juice_exe, output_filename
    );
    println!(
        "{:?}, {}, {}, {}",
        key_filenames, juice_exe, inter_file, output_filename
    );

    // Get executable
    let local_exe_path = String::from(MJ_FILES) + &juice_exe;
    get_file(LEADER_VM_IDX, &juice_exe, &local_exe_path).unwrap();

    for key_filename in key_filenames {
        // Get data file
        let localfile_path = String::from(MJ_FILES) + &key_filename;
        get_file(LEADER_VM_IDX, &key_filename, &localfile_path).unwrap();

        // Command to exec each time
        let empty: Vec<String> = Vec::new();
        let mut command = get_command(
            local_exe_path.clone(),
            &localfile_path,
            WORKER_TEMP_OUTPUT,
            empty,
        );

        command.output().unwrap();

        Command::new("rm").arg(localfile_path).output().unwrap();

        info!(
            "Successfully executed juice command for key = {}",
            key_filename
        )
    }

    // Write the output to sdfs
    let worker_id = get_vm_id();
    let sdfs_output_filename = output_filename.clone() + "_" + worker_id.to_string().as_str();
    put_file(LEADER_VM_IDX, WORKER_TEMP_OUTPUT, &sdfs_output_filename).unwrap();

    Command::new("rm").arg(WORKER_TEMP_OUTPUT).output().unwrap();

    // Open connection to leader
    let leader_addr = String::from(LEADER_VM_ID) + ":" + WL_PORT;
    let mut stream = TcpStream::connect(leader_addr).unwrap();

    stream.write(&[WL_JUICE_DONE, worker_id]).unwrap();
    write_str_to_stream(&output_filename, &mut stream).unwrap();
    write_str_to_stream(&sdfs_output_filename, &mut stream).unwrap();
}

fn replace_invalid_chars(filename: &str) -> String {
    filename.replace("/", "_")
}

fn get_command(
    local_exe_path: String,
    input: &str,
    output: &str,
    custom_params: Vec<String>,
) -> Command {
    Command::new("chmod")
        .arg("+x")
        .arg(&local_exe_path)
        .output()
        .unwrap();
    let mut command = Command::new(local_exe_path);
    command.arg(input).arg(output).args(custom_params);
    return command;
}

fn remove_maple_temp_files(filepaths: Vec<&str>) {
    for file in filepaths {
        Command::new("rm").arg(file).output().unwrap();
    }
}
