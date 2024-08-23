# CS425 - MP4


## Running instructions

1. Clone this repository on all desired machines
2. Create a file named 'client_id.txt' with an identifying index for your machine (we used the VM id on our tests, and so our client IDs were 1-10)
3. Alter the values in VM_LIST in lib.rs to match your machine's names
4. Install rust and cargo on all machines, run cargo build
5. Run the necessary scripts for the SDFS:
    1. Run `cargo run --bin sdfs-heartbeat` on every machine
    2. Run `cargo run --bin sdfs-datanode` on every machine that you want to use as a datanode
    3. Run `cargo run --bin sdfs-server` on any one machine
    4. Run `cargo run --bin sdfs-client` on any machine you want to use to access the SDFS. Use the following commands on your client program:
        1. `put localfilename sdfsfilename`: inserts a file from local directory into SDFS, returns a confirmation on success 
        2. `get sdfsfilename localfilename`: fetches a file from SDFS into local directory, returns a confirmation on success
        3. `ls sdfsfilename`: list all machine ids where this file is currently replicated
        4. `store`: list the set of file names that are replicated (stored) on SDFS at this (local) process/VM
6. Run the necessary scripts for the MapleJuice System:
    1. Run `cargo run --bin mj-heartbeat` on the leader VM (VM#1)
    2. Run `cargo run --bin mj-leader` on the leader VM (VM#1)
    3. Run `cargo run --bin mj-worker` on every machine except the leader
    4. Run `cargo run --bin mj-client` on any machine you want to use to access the MJ system. Use the following commands on your client program:
        1. `maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_filename> [<custom_params>]` to begin a custom Maple command
        2. `juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}` to begin a custom Juice command
        3. `sql SELECT ALL FROM sdfs_src_filename WHERE <regex> INTO sdfs_dest_filename IN <num_tasks> TASKS` to begin a specific SQL command