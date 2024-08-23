use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;

use mj::DELIMITER_1;

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("Args: {:?}", args);

    let input_filename = &args[1];
    let output_filename = &args[2];

    let input = fs::read_to_string(input_filename).unwrap();

    let mut output = String::new();
    let mut key = String::new();
    let mut sum = 0;

    for line in input.lines() {
        let items: Vec<&str> = line.split(",").collect();

        if key.is_empty() {
            key = String::from(items[0]);
        }

        let num = items[1].parse::<i32>().unwrap();
        sum += num;
    }

    output += "1,";
    output += &key;
    output += DELIMITER_1;
    output += sum.to_string().as_str();
    output += "\n";

    let mut output_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)
        .unwrap();
    output_file.write(output.as_bytes()).unwrap();
}
