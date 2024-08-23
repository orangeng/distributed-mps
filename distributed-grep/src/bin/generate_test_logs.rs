use rand::Rng;
use std::fs::File;
use std::io::{self, Write};

// constants used in test log files
pub const FREQUENT_PATTERN: &str = "this is a common pattern woohoo. 1234 !*[]!.\n";
pub const SOMEWHAT_FREQUENT_PATTERN: &str =
    "this is a somewhat frequent pattern woohoo. 1234 !*[]!.\n";
pub const RARE_PATTERN: &str = "this is a rare pattern wowee. 1234 !*[]!. &&&. \n";

use std::env;

fn main() {
    // Get the command-line arguments
    let args: Vec<String> = env::args().collect();

    // Check if there is at least one argument
    if args.len() < 2 {
        println!("Usage: {} <argument>", args[0]);
        return;
    }

    // Check the value of the first argument
    match args[1].as_str() {
        "trivial" => {
            let _ = generate_trivial_logs();
        }
        "real" => {
            let _ = generate_real_logs();
        }
        _ => {
            println!("Instructions: Use 'trivial' or 'real' arguments.");
        }
    }
}

fn generate_trivial_logs() -> io::Result<()> {
    for vm_number in 1..=10 {
        println!("Generating logs for VM#{}", vm_number);

        // Define the path to the file
        let file_path = format!("test_logs/trivial_tests/vm{}.log", vm_number);

        // Create the file
        let mut file = File::create(file_path)?;

        // Write frequent pattern to all files
        file.write_all(FREQUENT_PATTERN.as_bytes())?;

        // Write somewhat frequent pattern to half of the files
        if vm_number % 2 == 0 {
            file.write_all(SOMEWHAT_FREQUENT_PATTERN.as_bytes())?;
        }

        // Write rare pattern to only two files
        if vm_number == 1 || vm_number == 7 {
            file.write_all(RARE_PATTERN.as_bytes())?;
        }
    }

    Ok(())
}

fn generate_real_logs() -> io::Result<()> {
    for vm_number in 1..=10 {
        println!("Generating logs for VM#{}", vm_number);

        // Define the path to the file
        let file_path = format!("test_logs/real_tests/vm{}.log", vm_number);

        // Create the file
        let mut file = File::create(file_path)?;

        // Filling log files with 60MB worth of data
        let target_size_bytes = 60 * 1024 * 1024; // 60MB in bytes

        // Half the files will be filled with frequent pattern only,
        // the other half will get a mix of frequent and somewhat frequent patterns (in a random ratio).
        if vm_number % 2 != 0 {
            let string_size_bytes = FREQUENT_PATTERN.len();
            let num_repeats = target_size_bytes / string_size_bytes;

            for _ in 0..num_repeats {
                file.write_all(FREQUENT_PATTERN.as_bytes())?;
            }
        } else {
            // ratio of frequent:somewhat frequent lines is random
            let random_value: f64 = (rand::thread_rng().gen_range(0..100) as f64) / 100.0;

            let freq_string_size_bytes = FREQUENT_PATTERN.len();
            let num_freq_repeats =
                (target_size_bytes as f64 * random_value) / freq_string_size_bytes as f64;
            for _ in 0..(num_freq_repeats.ceil() as i32) {
                file.write_all(FREQUENT_PATTERN.as_bytes())?;
            }

            let some_freq_string_size_bytes = SOMEWHAT_FREQUENT_PATTERN.len();
            let num_some_freq_repeats: f64 = (target_size_bytes as f64 * (1.0 - random_value))
                / some_freq_string_size_bytes as f64;
            for _ in 0..(num_some_freq_repeats.ceil() as i32) {
                file.write_all(SOMEWHAT_FREQUENT_PATTERN.as_bytes())?;
            }
        }

        // Write rare pattern to only one files
        if vm_number == 1 {
            file.write_all(RARE_PATTERN.as_bytes())?;
        }
    }

    Ok(())
}
