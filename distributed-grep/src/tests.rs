use std::path::PathBuf;
use std::process::Command;

// USAGE: run either `cargo test real` or `cargo test trivial`, since only one set can work at a time with the server logs set up correctly.
// TODO: add back in option for adding flags to test

#[test]
fn trivial_frequent_pattern() {
    let (command_output, test_output) = setup_test_equality(String::from(""), false);
    assert_eq!(command_output, test_output)
}

#[test]
fn trivial_somewhat_frequent_pattern() {
    let (command_output, test_output) =
        setup_test_equality(String::from("a somewhat frequent pattern"), false);
    assert_eq!(command_output, test_output)
}

#[test]
fn trivial_rare_pattern() {
    let (command_output, test_output) = setup_test_equality(String::from("a rare pattern"), true);
    assert_eq!(command_output, test_output)
}

#[test]
fn real_frequent_pattern() {
    let (command_output, test_output) = setup_test_equality(String::from(""), true);
    assert_eq!(command_output, test_output)
}

#[test]
fn real_somewhat_frequent_pattern() {
    let (command_output, test_output) =
        setup_test_equality(String::from("a somewhat frequent pattern"), true);
    assert_eq!(command_output, test_output)
}

#[test]
fn real_rare_pattern() {
    let (command_output, test_output) = setup_test_equality(String::from("a rare pattern"), true);
    assert_eq!(command_output, test_output)
}

fn setup_test_equality(grep_file: String, real_logs: bool) -> (String, String) {
    // Running our command to grep from distributed log files
    let mut command_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    command_path.push("target/debug/client");
    let command_output_ = Command::new(command_path)
        .arg(grep_file.clone())
        .output()
        .expect("Failed to execute command");
    let command_output = String::from_utf8(command_output_.stdout).unwrap();

    (command_output, run_grep_locally(grep_file, real_logs))
}

// Creating expected result from running grep on log files locally
fn run_grep_locally(grep_file: String, real_logs: bool) -> String {
    let mut test_output = String::new();
    let mut test_count: i32 = 0;

    for vm_number in 1..=10 {
        test_output.push_str(&format!("\nVM #{}: [Online]\n", vm_number));

        let log_path: String;
        if real_logs {
            log_path = format!("./test_logs/real_tests/vm{}.log", vm_number)
        } else {
            log_path = format!("./test_logs/trivial_tests/vm{}.log", vm_number)
        }

        let test_output_ = Command::new("grep")
            .arg(grep_file.clone())
            .arg(log_path.clone())
            .output()
            .expect("Failed to execute command");
        test_output += &String::from_utf8(test_output_.stdout).unwrap();

        let count_output = Command::new("grep")
            .arg("-c") // TODO: again, have tests correctly mess with flags
            .arg(grep_file.clone())
            .arg(log_path)
            .output()
            .expect("Failed to execute command");
        let mut count_string = String::from_utf8(count_output.stdout).unwrap();
        utils::trim_newline(&mut count_string);
        test_output.push_str(&format!("Count: {}\n", count_string));
        let count: i32 = count_string.parse().unwrap();
        test_count += count;
    }

    test_output.push_str(&format!(
        "\nTotal count: {}\nDone reading all VM logs.\n",
        test_count
    ));

    test_output
}
