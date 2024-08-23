use std::time::Duration;

pub const HEARTBEAT_PORT: u16 = 50001;
pub const INTRO_HOSTNAME: &str = "fa23-cs425-5701.cs.illinois.edu";

// parameters to experiment with, starting with arbitrary values:
pub const TGOSSIP: Duration = Duration::from_millis(400);
pub const TFAIL: Duration = Duration::new(3, 0);
pub const TCLEANUP: Duration = Duration::new(4, 0);
pub const TSUSTIMEOUT: Duration = Duration::new(5, 0);
pub const TSWITCH: Duration = Duration::new(5, 0);

pub const MODE_CHANGE_COOLDOWN: Duration = Duration::new(10, 0);

pub const HOSTNAME_LENGTH: i64 = 100;

// number of peers to gossip to at a time (fixed)
pub const GOSSIP_NUM: usize = 3;

// parameters for each datagram
pub const DATAGRAM_LENGTH: usize = 70;
pub const HOSTNAME_OFFSET: usize = 0;
pub const PORTNUM_OFFSET: usize = 50;
pub const TIMESTAMP_OFFSET: usize = 52;
pub const HEARTBEAT_OFFSET: usize = 60;
pub const STATUS_OFFSET: usize = 64;
pub const INCNUM_OFFSET: usize = 65;
pub const MODE_OFFSET: usize = 69;

// parameter for debugging - rate of messages to "drop"
pub const MESSAGE_DROP_RATE: f32 = 0.0;
