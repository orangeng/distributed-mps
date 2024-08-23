use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::{FromRawFd, IntoRawFd};
use std::process::{Command, Stdio};

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("Args: {:?}", args);

    let temp_output_file = "temp";

    let input_filename = &args[1];
    let output_filename = &args[2];

    let regex = &args[3];

    // println!("regex = {}", regex);

    // Open output file
    let output_file_fd;
    match File::create(temp_output_file) {
        Ok(file) => {
            output_file_fd = file.into_raw_fd();
        }
        Err(err) => {
            println!("Couldn't open output file: {}", err);
            return;
        }
    }
    let file_out = unsafe { Stdio::from_raw_fd(output_file_fd) };

    // Create and run grep command
    let mut command = Command::new("grep")
        .arg("-P")
        .arg(regex)
        .arg(input_filename)
        .stdout(file_out)
        .spawn()
        .unwrap();
    if let Err(err) = command.wait() {
        println!("Error running grep: {}", err);
    } else {
    }

    let file = File::open(temp_output_file).unwrap();
    let mut output_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)
        .unwrap();
    let buf_read = BufReader::new(file);
    for line in buf_read.lines() {
        let line = line.unwrap();
        let output_line = String::from("1,") + line.as_str() + "\n";
        output_file.write(output_line.as_bytes()).unwrap();
    }

    Command::new("rm").arg(temp_output_file).output().unwrap();
}
