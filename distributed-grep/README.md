# CS425 - MP1
By Liza George (lizag2), Quan Hao Ng (qhng2)


## Running Instructions

1. Open all ten VMs and run `git pull` to access desired code and generated log files.
2. Install [rust and cargo](https://www.rust-lang.org/tools/install), run `cargo build`.
3. To serve the trivial log files, run `cp ./test_logs/trivial_tests/vm[vm_num].log ./` in the project's root directory in each VM. To serve the real/long log files, run `cp ./test_logs/real_tests/vm[vm_num].log ./` instead.
4. Run `cargo run --bin server`.
5. On desired client VM, run `./target/debug/client "[grep args]" "[grep string]"` (eg ``./target/debug/client "-i" "Linux i686"`).

## Testing Instructions
1. Install [rust and cargo](https://www.rust-lang.org/tools/install), run `cargo build`.
2. Open all ten VMs, git pull, and run `cargo run --bin server`.
3. To test with the trivial log files, run `cp ./test_logs/trivial_tests/vm[vm_num].log ./` in the project's root directory in each VM. To serve the real/long log files, run `cp ./test_logs/real_tests/vm[vm_num].log ./` instead.
3. Run `cargo test real` if you're testing with the real logs, or `cargo test trivial` if you're testing with the trivial logs. 