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
    let mut sum = 0;
    let mut components: Vec<(&str, i32)> = Vec::new();

    for line in input.lines() {
        let items: Vec<&str> = line.split(",").collect();
        let key_value: Vec<&str> = items[1].split(DELIMITER_1).collect();

        let num = key_value[1].parse::<i32>().unwrap();
        components.push((key_value[0], num));
        sum += num;
    }

    for (key, value) in components {
        output += key;
        output += ",";
        output += (value as f64 / sum as f64).to_string().as_str();
        output += "\n";
    }

    let mut output_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)
        .unwrap();
    output_file.write(output.as_bytes()).unwrap();
}
