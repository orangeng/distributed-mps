extern crate log;

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Error, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;

use log::{error, info};

use mj::*;
use sdfs::interface::get_file;
use sdfs::put_file;

fn main() {
    setup_logger();

    // Key is the intermediate files prefix. E.g. test1
    // Value is Vec<key_file>
    let job_keys: HashMap<String, Vec<String>> = HashMap::new();
    let shared_job_keys = Arc::new(Mutex::new(job_keys));

    // Key is (inter_file, key)
    // Value is vector of intermediate keyfiles. E.g. [test1_2_Loop, test1_2_Radio]
    let key_from_each_worker: HashMap<(String, String), Vec<String>> = HashMap::new();
    let shared_inter_keys = Arc::new(Mutex::new(key_from_each_worker));

    // Key is inter_file
    // Value is list of workers
    let ongoing_jobs: HashMap<String, Vec<u8>> = HashMap::new();
    let shared_ongoing_jobs = Arc::new(Mutex::new(ongoing_jobs));

    // Key is inter file
    // Value is list of reduce outputs
    let reduce_outputs: HashMap<String, Vec<String>> = HashMap::new();
    let shared_reduce_outputs = Arc::new(Mutex::new(reduce_outputs));

    // <----------------- LISTENERS ----------------- >

    let shared_ongoing_jobs_client = shared_ongoing_jobs.clone();
    let shared_job_keys_client = shared_job_keys.clone();
    let client_listener =
        thread::spawn(move || client_listen(shared_ongoing_jobs_client, shared_job_keys_client));

    let shared_job_keys_worker = shared_job_keys.clone();
    let shared_ongoing_jobs_worker = shared_ongoing_jobs.clone();
    let shared_inter_keys_worker = shared_inter_keys.clone();
    let shared_reduce_outputs_worker = shared_reduce_outputs.clone();
    let worker_listener = thread::spawn(move || {
        worker_listen(
            shared_ongoing_jobs_worker,
            shared_inter_keys_worker,
            shared_job_keys_worker,
            shared_reduce_outputs_worker,
        )
    });

    client_listener.join().unwrap();
    worker_listener.join().unwrap();

    // TODO: start up thread that listens for messages from worker (worker responses should fill up job_keys)

    // Example usage:
    info!("This is an new informational message");
    error!("This is an error message");
}

// <----------------- CLIENT INTERACTIONS ----------------- >
// listen for messages from client, distinguish between new maple command (call handler), new juice command (call handler), sql command (call handler)
fn client_listen(
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    job_keys: Arc<Mutex<HashMap<String, Vec<String>>>>,
) {
    let listen_addr = String::from("0:") + CL_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut request_type: [u8; 1] = [0];
            if stream.read_exact(&mut request_type).is_err() {
                continue;
            };

            match request_type[0] {
                // Maple Command Request
                MAPLE_TYPE_ID => {
                    let ongoing_jobs_clone = ongoing_jobs.clone();
                    thread::spawn(move || {
                        if let Err(err) = handle_maple_req(&mut stream, ongoing_jobs_clone) {
                            error!("Error handling maple request = {}", err);
                            stream.write(&[ERROR_STATUS_CODE]).unwrap();
                        } else {
                            stream.write(&[SUCCESS_STATUS_CODE]).unwrap();
                        }
                    });
                }
                // Juice Command Request
                JUICE_TYPE_ID => {
                    let shared_job_keys = job_keys.clone();
                    let ongoing_job_juice = ongoing_jobs.clone();
                    thread::spawn(move || {
                        if let Err(err) =
                            handle_juice_req(&mut stream, shared_job_keys, ongoing_job_juice)
                        {
                            error!("Error handling juice request = {}", err);
                            stream.write(&[ERROR_STATUS_CODE]).unwrap();
                        } else {
                            stream.write(&[SUCCESS_STATUS_CODE]).unwrap();
                        }
                    });
                }
                _ => {}
            }
        } else {
            continue;
        }
    }
}

// <----------------- WORKER INTERACTIONS ----------------- >
// TODO: listen for messages from worker, distinguish between finished maple task (?? unblock handle maple?), finished juice task (?? unblock handle juice?)

fn worker_listen(
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    shared_inter_keys: Arc<Mutex<HashMap<(String, String), Vec<String>>>>,
    job_keys: Arc<Mutex<HashMap<String, Vec<String>>>>,
    reduce_outputs: Arc<Mutex<HashMap<String, Vec<String>>>>,
) {
    let listen_addr = String::from("0:") + WL_PORT;
    let listener = TcpListener::bind(listen_addr).unwrap();

    for conn_res in listener.incoming() {
        if let Ok(mut stream) = conn_res {
            let mut request_type: [u8; 1] = [0];
            if stream.read_exact(&mut request_type).is_err() {
                continue;
            };

            match request_type[0] {
                WL_MAPLE_DONE => {
                    let ongoing_jobs_maple_done = ongoing_jobs.clone();
                    let shared_inter_keys_maple_done = shared_inter_keys.clone();
                    let job_keys_maple_done = job_keys.clone();
                    thread::spawn(move || {
                        handle_maple_reply(
                            stream,
                            ongoing_jobs_maple_done,
                            shared_inter_keys_maple_done,
                            job_keys_maple_done,
                        )
                    });
                }
                WL_JUICE_DONE => {
                    let ongoing_jobs_juice_done = ongoing_jobs.clone();
                    let reduce_outputs_juice_done = reduce_outputs.clone();
                    thread::spawn(move || {
                        handle_juice_reply(
                            stream,
                            ongoing_jobs_juice_done,
                            reduce_outputs_juice_done,
                        )
                    });
                }
                _ => {}
            }
        } else {
            continue;
        }
    }
}

// handle maple - partition input (by lines), delegate to available workers, send tasks to workers, listen for response and send response to client when done
fn handle_maple_req(
    mut stream: &mut TcpStream,
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
) -> Result<String, Error> {
    // Read request from stream
    let num_maples = read_u8_from_stream(&mut stream)? as usize;
    let _ = read_u8_from_stream(&mut stream)?; // Don't care about delete_input
    let maple_exe = read_str_from_stream(&mut stream)?;
    let inter_file_prefix = read_str_from_stream(&mut stream)?;
    let sdfs_src_filename = read_str_from_stream(&mut stream)?;
    let custom_params = read_custom_params(&mut stream)?;

    info!(
        "Leader: Received maple request: {},{},{},{},{:?}",
        maple_exe, num_maples, inter_file_prefix, sdfs_src_filename, custom_params
    );
    println!(
        "{},{},{},{},{:?}",
        maple_exe, num_maples, inter_file_prefix, sdfs_src_filename, custom_params
    );

    // Find num_maple available workers
    let maple_worker_idxs = get_available_workers(num_maples)?;

    // Find input size - examine input file using SDFS interface
    if let Err(err) = get_file(
        LEADER_VM_IDX,
        &sdfs_src_filename,
        (MJ_FILES.to_string() + &sdfs_src_filename).as_str(),
    ) {
        error!("Couldn't get sdfs input file: {}", err);
        return Err(Error::new(
            ErrorKind::Other,
            "Error in getting input file from SDFS: ".to_string() + &err.to_string(),
        ));
    };
    let local_sdfs_filename = MJ_FILES.to_string() + &sdfs_src_filename;

    let mut input_size = 0;
    if let Ok(file) = fs::File::open(&local_sdfs_filename) {
        let reader = io::BufReader::new(file);
        input_size += io::BufRead::lines(reader).count();
    }

    if input_size == 0 {
        return Err(Error::new(ErrorKind::Other, "Invalid SDFS input file"));
    }
    input_size -= 1; // Don't want to count first line of CSV defining schema
    println!("Input size: {}", input_size);

    // delegate between workers (range partitioning) and send tasks to workers
    let task_size = input_size / num_maples;
    for i in 0..num_maples {
        let worker_id = VM_LIST[maple_worker_idxs[i] as usize];

        let range_start = (i * task_size) as i32 + 1;
        let mut range_end = ((i + 1) * task_size - 1) as i32 + 1;
        if i == num_maples - 1 {
            range_end = (input_size - 1) as i32 + 1;
        }

        // send tasks to workers
        let sock_addr = String::from(worker_id) + ":" + LW_PORT;
        let mut stream = TcpStream::connect(sock_addr)?;

        let to_send = [MAPLE_TYPE_ID];
        stream.write(&to_send)?;
        write_i32_to_stream(range_start, &mut stream)?;
        write_i32_to_stream(range_end, &mut stream)?;

        write_str_to_stream(&maple_exe, &mut stream)?;
        write_str_to_stream(&inter_file_prefix, &mut stream)?;
        write_str_to_stream(&sdfs_src_filename, &mut stream)?;
        write_custom_params(
            custom_params.iter().map(|x| x.as_str()).collect(),
            &mut stream,
        )
        .unwrap();
    }

    // Update ongoing jobs
    {
        let mut ongoing_jobs = ongoing_jobs.lock().unwrap();
        ongoing_jobs
            .entry(inter_file_prefix)
            .or_insert(maple_worker_idxs);
        println!("Ongoing jobs: {:?}", ongoing_jobs);
    }
    return Ok("Success".to_string());
}

fn handle_maple_reply(
    mut stream: TcpStream,
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    shared_inter_keys: Arc<Mutex<HashMap<(String, String), Vec<String>>>>,
    job_keys: Arc<Mutex<HashMap<String, Vec<String>>>>,
) {
    let worker_id = read_u8_from_stream(&mut stream).unwrap();
    let inter_file = read_str_from_stream(&mut stream).unwrap();
    let key_filename = read_key_filename_tuple(&mut stream).unwrap();

    println!(
        "Received reply: {}, {}, {:?}",
        worker_id, inter_file, key_filename
    );

    // Update shared_inter_keys
    {
        let mut shared_inter_keys = shared_inter_keys.lock().unwrap();

        for (key, filename) in key_filename {
            shared_inter_keys
                .entry((inter_file.clone(), key))
                .and_modify(|filename_vec| {
                    filename_vec.push(filename.clone());
                })
                .or_insert(vec![filename]);
        }
        println!("inter_keys: {:?}", shared_inter_keys);
    }

    // Update shared_ongoing_jobs
    let mut coalesce_keyfiles = false;
    {
        let mut ongoing_jobs = ongoing_jobs.lock().unwrap();
        let workers = ongoing_jobs.get_mut(&inter_file).unwrap();
        for i in 0..workers.len() {
            if workers[i] == (worker_id - 1) {
                workers.remove(i);
                break;
            }
        }
        if workers.is_empty() {
            coalesce_keyfiles = true;
            ongoing_jobs.remove(&inter_file);
        }
        println!("ongoing_jobs: {:?}", ongoing_jobs);
    }

    // Merge files
    if coalesce_keyfiles {
        let mut inter_keys = shared_inter_keys.lock().unwrap();

        let mut job_keys = job_keys.lock().unwrap();
        let keys_vec = job_keys.entry(inter_file.clone()).or_default();
        let mut to_remove: Vec<(String, String)> = Vec::new();
        for ((inter, key), files) in inter_keys.iter() {
            // Check if entry is for this job
            if inter_file.eq(inter) {
                to_remove.push((inter.clone(), key.clone()));
                let final_keyfile_name = inter_file.clone() + "_" + key.as_str();
                let mut final_file =
                    File::create(String::from(MJ_FILES) + final_keyfile_name.as_str()).unwrap();
                for sdfs_file in files {
                    get_file(
                        LEADER_VM_IDX,
                        &sdfs_file,
                        &(String::from(MJ_FILES) + &sdfs_file),
                    )
                    .unwrap();
                    final_file
                        .write(
                            fs::read_to_string(String::from(MJ_FILES) + sdfs_file)
                                .unwrap()
                                .as_bytes(),
                        )
                        .unwrap();
                    Command::new("rm")
                        .arg(String::from(MJ_FILES) + sdfs_file)
                        .output()
                        .unwrap();
                }
                keys_vec.push(final_keyfile_name.clone());
                put_file(
                    LEADER_VM_IDX,
                    &(String::from(MJ_FILES) + &final_keyfile_name),
                    &final_keyfile_name,
                )
                .unwrap();
            }
        }

        println!("job keys: {:?}", job_keys);

        for key in to_remove {
            inter_keys.remove(&key);
        }

        println!("inter keys: {:?}", inter_keys);
    }
}

// TODO: handle juice - partition input (by sets of keys), delegate to available workers, send tasks to workers, listen for response and send response to client when done. delete intermediate files when done if specified
fn handle_juice_req(
    mut stream: &mut TcpStream,
    job_keys: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
) -> Result<String, Error> {
    // Read request from stream
    let num_juices = read_u8_from_stream(&mut stream)? as usize;
    let delete_input = read_u8_from_stream(&mut stream)?; // Don't care about delete_input
    let juice_exe = read_str_from_stream(&mut stream)?;
    let inter_file_prefix = read_str_from_stream(&mut stream)?;
    let sdfs_dest_filename = read_str_from_stream(&mut stream)?;

    info!(
        "Leader: Received juice request: {},{},{},{},{:?}",
        juice_exe, num_juices, delete_input, inter_file_prefix, sdfs_dest_filename
    );
    println!(
        "Leader: Received juice request: {},{},{},{},{:?}",
        juice_exe, num_juices, delete_input, inter_file_prefix, sdfs_dest_filename
    );

    // Find num_juice available workers
    let juice_worker_idxs = get_available_workers(num_juices)?;

    // Find input size - use job_keys dict
    let keys_list: Vec<String>;
    {
        let job_keys_locked = job_keys.lock().unwrap();
        if let Some(val) = job_keys_locked.get(&inter_file_prefix) {
            keys_list = val.to_vec();
        } else {
            error!(
                "Coudn't find any keys for this juice task: {}",
                inter_file_prefix
            );
            return Err(Error::new(
                ErrorKind::Other,
                "Coudn't find any keys for this juice task: ".to_string() + &inter_file_prefix,
            ));
        }
    }
    let input_size = keys_list.len();
    println!("Input size: {}", input_size);

    let mut task_size = input_size / num_juices;
    if task_size == 0{task_size = 1;}

    // delegate keys between workers (range partitioning), send tasks to workers
    let mut used_workers: Vec<u8> = Vec::new();
    for i in 0..num_juices {
        let worker_id = VM_LIST[juice_worker_idxs[i] as usize];

        let range_start = i * task_size;
        if (range_start + 1) > input_size{
            break;
        }
        let mut range_end = (i + 1) * task_size - 1;
        if i == (num_juices - 1) {
            range_end = input_size - 1;
        }
        let keys_list_str = keys_list[range_start..range_end + 1].join(",");
        info!(
            "Sending task to worker {} with keys_list_str = {}",
            worker_id, keys_list_str
        );

        // send tasks to workers
        used_workers.push(juice_worker_idxs[i]);
        let sock_addr = String::from(worker_id) + ":" + LW_PORT;
        let mut stream = TcpStream::connect(sock_addr)?;

        let to_send: [u8; 1] = [JUICE_TYPE_ID];
        stream.write(&to_send)?;
        write_str_to_stream(&keys_list_str, &mut stream)?;
        write_str_to_stream(&juice_exe, &mut stream)?;
        write_str_to_stream(&inter_file_prefix, &mut stream)?;
        write_str_to_stream(&sdfs_dest_filename, &mut stream)?;
    }

    // Update ongoing jobs
    {
        let mut ongoing_jobs = ongoing_jobs.lock().unwrap();
        ongoing_jobs
            .entry(sdfs_dest_filename)
            .or_insert(used_workers);
        println!("Ongoing jobs: {:?}", ongoing_jobs);
    }

    return Ok("Success".to_string());
}

fn handle_juice_reply(
    mut stream: TcpStream,
    ongoing_jobs: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    reduce_outputs: Arc<Mutex<HashMap<String, Vec<String>>>>,
) {
    let worker_id = read_u8_from_stream(&mut stream).unwrap();
    let output_filename = read_str_from_stream(&mut stream).unwrap();
    let worker_output = read_str_from_stream(&mut stream).unwrap();

    println!(
        "Juice reply: {}, {}, {}",
        worker_id, output_filename, worker_output
    );

    // Update reduce_outputs
    {
        let mut reduce_outputs = reduce_outputs.lock().unwrap();
        reduce_outputs
            .entry(output_filename.clone())
            .and_modify(|out_vec| {
                out_vec.push(worker_output.clone());
            })
            .or_insert(vec![worker_output]);

        println!("reduce_output: {:?}", reduce_outputs);
    }

    // Update shared_ongoing_jobs
    let mut coalesce_reducefiles = false;
    {
        let mut ongoing_jobs = ongoing_jobs.lock().unwrap();
        let workers = ongoing_jobs.get_mut(&output_filename).unwrap();
        for i in 0..workers.len() {
            if workers[i] == (worker_id - 1) {
                workers.remove(i);
                break;
            }
        }
        if workers.is_empty() {
            coalesce_reducefiles = true;
            ongoing_jobs.remove(&output_filename);
        }
        println!("ongoing_jobs: {:?}", ongoing_jobs);
    }

    // Coalesce files
    if coalesce_reducefiles {
        let mut reduce_output = reduce_outputs.lock().unwrap();
        let output_vec = reduce_output.entry(output_filename.clone()).or_default();
        let mut local_finaloutput =
            File::create(String::from(MJ_FILES) + &output_filename).unwrap();
        for worker_output in output_vec {
            let local_workeroutput = String::from(MJ_FILES) + worker_output;
            get_file(LEADER_VM_IDX, &worker_output, &local_workeroutput).unwrap();
            local_finaloutput
                .write(
                    fs::read_to_string(local_workeroutput.clone())
                        .unwrap()
                        .as_bytes(),
                )
                .unwrap();
            Command::new("rm").arg(local_workeroutput).output().unwrap();
        }
        put_file(
            LEADER_VM_IDX,
            &(String::from(MJ_FILES) + &output_filename),
            &output_filename,
        )
        .unwrap();
    }
}

// TODO: handle sql command - call handle maple?? TODO: figure this out bruh

// <----------------- WORKER HELPER FUNCTIONS ----------------- >
// find num_maple available workers
fn get_available_workers(num_tasks: usize) -> Result<Vec<u8>, Error> {
    let available_worker_idxs = get_membership();
    let mut chosen_nodes: Vec<u8> = Vec::new();
    let mut count = 0;

    // Skipping id 1 because workers should be non-vm nodes, hardcoding first VM as leader
    for i in 1..available_worker_idxs.len() {
        if available_worker_idxs[i] == 1 {
            chosen_nodes.push(i as u8);
            count += 1;
            if count == num_tasks {
                break;
            }
        }
    }

    if count < num_tasks {
        return Err(Error::new(ErrorKind::Other, "Not enough available nodes"));
    }
    return Ok(chosen_nodes);
}
