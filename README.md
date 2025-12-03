Project name
- TCP P2P File Sharing

Teammates
- Dominick Dupuy
- Phillip-Dylan Ndwiga
- Hemdutt Rao
*Work was evenly split amongst teammates

Language and entry point
- Python 3
- Main program: pp.py

Project contents
- pp.py                 # peer process implementation
- README.md             # Definition and instructions
- tcp_config_small/     # small test configuration (Common.cfg, PeerInfo.cfg, peer_* dirs)
- tcp_config_large/     # large test configuration (Common.cfg, PeerInfo.cfg, peer_* dirs)
- log_peer_*.log        # per-peer logs generated at runtime

Config files
- Common.cfg
  - NumberOfPreferredNeighbors
  - UnchokingInterval
  - OptimisticUnchokingInterval
  - FileName
  - FileSize
  - PieceSize
- PeerInfo.cfg
  - Columns: peerID hostname port hasFile
  - hasFile = 1 for a seed, 0 for a leecher

Running on a single machine
1) Change into the project directory:
   cd path/to/project

2) Initialize or reset the peers for a config:
   python pp.py --config-dir tcp_config_small --reset
   -- or --
   python pp.py --config-dir tcp_config_large --reset

   This reads Common.cfg and PeerInfo.cfg, splits the source file into pieces,
   distributes them to seed peers, clears leecher files, and resets internal state.

3) Start each peer in its own terminal using its peerID:
   python pp.py 1001 --config-dir tcp_config_small --no-reset
   python pp.py 1002 --config-dir tcp_config_small --no-reset
   python pp.py 1003 --config-dir tcp_config_small --no-reset
   ...

   - The numeric argument must match a peerID in PeerInfo.cfg.
   - Use the same --config-dir for all peers in a run.
   - Use --no-reset during a run so one peer exiting does not reset others.

4) Let the peers run until every log file contains the line:
   "Peer [peer_ID] has downloaded the complete file."
   At that point, the processes terminate cleanly.

Running on multiple machines
1) Copy the entire project directory to each machine.

2) Edit the PeerInfo.cfg inside the chosen config directory so that the hostname field
   for each peer is the IP or hostname of the machine that will run that peer. Example:

   1001 [Machine 1 IP] 7001 1
   1002 [Machine 1 IP] 7002 0
   1003 [Machine 2 IP] 7003 0
   1004 [Machine 2 IP] 7001 0
   1005 [Machine 3 IP] 7002 0
   1006 [Machine 3 IP] 7003 0

   Use the same Common.cfg and PeerInfo.cfg on all machines.

3) On each machine, initialize/reset once:
   python pp.py --config-dir tcp_config_small --reset

4) On each machine, start only the peers assigned to that machine, with --no-reset. Example:

   # Machine A
   python pp.py 1001 --config-dir tcp_config_small --no-reset
   python pp.py 1002 --config-dir tcp_config_small --no-reset

   # Machine B
   python pp.py 1003 --config-dir tcp_config_small --no-reset
   python pp.py 1004 --config-dir tcp_config_small --no-reset

   # Machine C
   python pp.py 1005 --config-dir tcp_config_small --no-reset
   python pp.py 1006 --config-dir tcp_config_small --no-reset

Protocol and implementation overview
- Handshake
  - 32-byte handshake per connection:
    - 18-byte protocol string "P2PFILESHARINGPROJ"
    - 10 reserved zero bytes
    - 4-byte peer ID (big-endian)
  - Both sides validate the protocol string and peer ID before continuing.

- Message framing
  - All messages have format: [4-byte length][1-byte type][payload].
  - Implemented types:
    - 0: choke
    - 1: unchoke
    - 2: interested
    - 3: not interested
    - 4: have
    - 5: bitfield
    - 6: request
    - 7: piece

- Bitfield and interest
  - After handshake, peers exchange a bitfield of owned pieces.
  - A peer sends INTERESTED if a neighbor has at least one piece it does not have.
  - If no such pieces remain, it sends NOT INTERESTED.
  - On each HAVE message, the neighbor’s bitfield is updated and interest is recomputed.

- Choking and unchoking
  - Each peer tracks which neighbors are interested in it.
  - Every UnchokingInterval seconds:
    - For interested neighbors, download rates are measured.
    - The top NumberOfPreferredNeighbors become preferred and are unchoked
      (or chosen randomly if this peer already has the complete file).
    - All other neighbors are choked, except for the optimistic neighbor.
  - Every OptimisticUnchokingInterval seconds:
    - One choked but interested neighbor is selected at random as the optimistic unchoke.

- Piece requests and transfer
  - When a peer is unchoked by a neighbor and is interested:
    - It chooses a piece index that:
      - It does not have,
      - The neighbor has,
      - It has not already requested from other neighbors.
    - It sends a REQUEST for that piece index.
  - On receiving a REQUEST, a peer replies with a PIECE message containing the piece data.
  - On receiving a PIECE:
    - The piece is stored,
    - A HAVE message is broadcast to neighbors,
    - The peer attempts to issue another REQUEST if it remains unchoked and interested.
  - At most one REQUEST per neighbor is outstanding at a time.

- Starvation protection
  - A watchdog checks for requests that have been outstanding for too long and can
    re-issue a REQUEST so that a peer does not get stuck on a stalled transfer.

- Completion and shutdown
  - Each peer tracks its own bitfield and knows when it has all pieces.
  - When complete:
    - It reconstructs and writes the full file into its peer_<peerID> directory
      inside the chosen config directory.
    - It logs that it has downloaded the complete file.
  - When all peers are known to be complete, peers exit cleanly.

Logging
- Each peer writes to log_peer_<peerID>.log in the current working directory.
- Logs include:
  - Connection establishment:
    - "Peer X makes a connection to Peer Y."
    - "Peer X is connected from Peer Y."
  - Preferred neighbor changes.
  - Optimistically unchoked neighbor changes.
  - Choke and unchoke events.
  - INTERESTED and NOT INTERESTED messages.
  - HAVE messages.
  - Piece downloads:
    - "Peer X has downloaded the piece i from Y. Now the number of pieces it has is N."
  - Completion:
    - "Peer X has downloaded the complete file."

Demo video
- https://youtu.be/17R6_tzqE2M