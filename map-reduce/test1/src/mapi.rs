use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("Args: {:?}", args);

    let input_filename = &args[1];
    let output_filename = &args[2];

    let input = fs::read_to_string(input_filename).unwrap();

    let mut output_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)
        .unwrap();
    output_file.write(input.as_bytes()).unwrap();
}
