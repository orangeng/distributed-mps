use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;

fn main() {
    let args: Vec<String> = env::args().collect();

    println!("Args: {:?}", args);

    let input_filename = &args[1];
    let output_filename = &args[2];

    let x = &args[3];

    let input = fs::read_to_string(input_filename).unwrap();

    let mut output: String = String::new();

    for line in input.lines() {
        let items: Vec<&str> = line.split(",").collect();

        // Column 11 is 'Interconne'
        if items[10] == x {
            // Column 10 is
            output += items[9];
            output += ",1";
            output += "\n";
        }
    }
    let mut output_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(output_filename)
        .unwrap();
    output_file.write(output.as_bytes()).unwrap();
}
