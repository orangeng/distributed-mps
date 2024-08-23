# CS425 - MP2


## Message Protocol
- Each message represents an update for one entry in a membership list. The total buffer is always HOSTNAME_LENGTH + 16 bytes (HOSTNAME_LENGTH is a parameter in `lib.rs`)
- the first HOSTNAME_LENGTH + 10 bytes is the peer's ID; the first HOSTNAME_LENGTH are for the hostname (followed by a deliminatin `:` character), followed by 2 bytes for the port number, another `:` character, and 8 bytes for the timestamp
- the next 4 bytes are for the peer's heartbeat counter. If these 4 bytes are 0, that indicates that this peer is voluntarily leaving
- if the sending machine is using Gossip, the next 5 bytes are unecessary. If it is using Gossip-S, the next byte indicate the state of this peer (0 for Alive, 1 for Failed, and 2 for Suspected), and the 4 bytes are for the incarnation number.
- the last byte is the mode byte, indicating if the sending machine is using the Gossip-S or Gossip mode.
    - if Switch Byte = 1, this message was sent from a peer using Gossip-S, else it was sent from a peer using Gossip. If the receiving machine is using the opposite system of this peer, and is not in the TSWITCH cooldown period, this machine will switch which system it uses too.