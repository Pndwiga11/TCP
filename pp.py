import socket
import threading
import time
import random
import signal
import sys
from pprint import pprint
from pathlib import Path
import argparse
from collections import defaultdict, deque

CONFIG_DIR: Path | None = None

# --- Protocol constants ---
PSTR = b"P2PFILESHARINGPROJ"          # 18 bytes
HS_RESERVED = b"\x00" * 10            # 10 bytes

# message type ids
MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOTINTERESTED = 3
MSG_HAVE = 4
MSG_BITFIELD = 5
MSG_REQUEST = 6
MSG_PIECE = 7

def find_config_dir(explicit: str | None) -> Path:
    """
    Decide which directory to use for Common.cfg and PeerInfo.cfg.
    Priority:
      1) --config-dir value if given
      2) current directory
      3) ./tcp_config_small
      4) ./tcp_config_large
    """
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))

    candidates.append(Path("."))  # spec-style layout
    candidates.append(Path("tcp_config_small"))
    candidates.append(Path("tcp_config_large"))

    for base in candidates:
        if (base / "Common.cfg").exists() and (base / "PeerInfo.cfg").exists():
            return base

    raise FileNotFoundError(
        "Could not find Common.cfg and PeerInfo.cfg. "
        "Tried: " + ", ".join(str(c) for c in candidates)
    )



# --- Socket helpers ---
def send_all(sock, data):
    view = memoryview(data)
    while view:
        try:
            n = sock.send(view)
        except OSError as e:
            return

        if n == 0:
            return

        view = view[n:]

def recv_exact(sock, n) -> bytes:
    buf = bytearray(n)
    view = memoryview(buf)
    while view:
        k = sock.recv_into(view)
        if k == 0:
            raise ConnectionError("socket closed")
        view = view[k:]
    return bytes(buf)

# Handshake 32 bytes
def send_handshake(sock, my_peer_id):
    payload = PSTR + HS_RESERVED + my_peer_id.to_bytes(4, "big", signed=False)
    send_all(sock, payload)

def recv_handshake(sock) -> int:
    data = recv_exact(sock, 32)
    header = data[:18]
    reserved = data[18:28]
    pid = int.from_bytes(data[28:32], "big", signed=False)
    if header != PSTR or reserved != HS_RESERVED:
        raise ValueError("bad handshake")
    return pid

# length-prefixed frame
def send_frame(sock, msg_type, payload = b""):
    total_len = 1 + len(payload)
    send_all(sock, total_len.to_bytes(4, "big") + bytes([msg_type]) + payload)

def recv_frame(sock):
    length = int.from_bytes(recv_exact(sock, 4), "big")
    if length == 0:
        return 0, b""
    msg_type = recv_exact(sock, 1)[0]
    payload = recv_exact(sock, length - 1) if length > 1 else b""
    return msg_type, payload

def build_bitfield_bytes(my_piece_set, total_pieces):
    bits = ['1' if i in my_piece_set else '0' for i in range(total_pieces)]
    while len(bits) % 8 != 0:
        bits.append('0')
    out = bytearray()
    for i in range(0, len(bits), 8):
        out.append(int(''.join(bits[i:i+8]), 2))
    return bytes(out)

def parse_bitfield_bytes(b, total_pieces):
    have = set()
    bitidx = 0
    for byte in b:
        for shift in range(7, -1, -1):
            if bitidx >= total_pieces:
                return have
            if (byte >> shift) & 1:
                have.add(bitidx)
            bitidx += 1
    return have

def compute_and_send_interest(my_id, sock, neighbor_id, neighbor_have_hint=None):
    """
    Decide whether I'm interested in neighbor_id.
    neighbor_have_hint: optional fresh 'have' set from a just-received
    BITFIELD or HAVE message. If not provided, we fall back to the
    neighbor_bitfields table.
    """
    with distribution_state['lock']:
        my_have = distribution_state['peer_pieces'].get(my_id, set())

        if neighbor_have_hint is not None:
            neighbor_have = set(neighbor_have_hint)
        else:
            neighbor_have = distribution_state['neighbor_bitfields'].get(my_id, {}).get(neighbor_id, set())

    need = neighbor_have - my_have

    if need:
        send_frame(sock, MSG_INTERESTED, b"")
        with distribution_state['lock']:
            distribution_state['am_interested_in'].setdefault(my_id, set()).add(neighbor_id)
        print(f"[Peer {my_id}] -> INTERESTED to {neighbor_id} (need {len(need)})")
        log_peer(my_id, f"Peer {my_id} has sent the 'interested' message to Peer {neighbor_id}.")
    else:
        send_frame(sock, MSG_NOTINTERESTED, b"")
        with distribution_state['lock']:
            distribution_state['am_interested_in'].setdefault(my_id, set()).discard(neighbor_id)
        print(f"[Peer {my_id}] -> NOT INTERESTED to {neighbor_id}")
        log_peer(my_id, f"Peer {my_id} has sent the 'not interested' message to Peer {neighbor_id}.")



def set_choke_state(my_id, neighbor_id, sock, choked: bool):
    if choked:
        send_frame(sock, MSG_CHOKE, b"")
        with distribution_state['lock']:
            distribution_state['unchoked_out'][my_id].discard(neighbor_id)
        print(f"[Peer {my_id}] -> CHOKE to {neighbor_id}")
    else:
        send_frame(sock, MSG_UNCHOKE, b"")
        with distribution_state['lock']:
            distribution_state['unchoked_out'][my_id].add(neighbor_id)
        print(f"[Peer {my_id}] -> UNCHOKE to {neighbor_id}")

def request_watchdog(my_id, timeout=15):
    while not shutdown_flag:
        time.sleep(1)
        with distribution_state['lock']:
            infl = dict(distribution_state['inflight'].get(my_id, {}))
        now = time.time()
        for neighbor_id, val in infl.items():
            if not isinstance(val, tuple) or len(val) != 2:
                continue
            piece_idx, ts = val
            if now - ts > timeout:
                s = sock_for(my_id, neighbor_id)
                if not s:
                    continue
                payload = piece_idx.to_bytes(4, 'big', signed=False)
                try:
                    send_frame(s, MSG_REQUEST, payload)
                    with distribution_state['lock']:
                        distribution_state['inflight'][my_id][neighbor_id] = (piece_idx, now)
                    print(f"[Peer {my_id}] RETRY piece {piece_idx} with {neighbor_id}")
                except Exception:
                    pass

def receiver_loop(peer_id, sock, remote_peer_id_or_addr, outgoing_connections, incoming_connections):
    sock.settimeout(1.0)
    while not shutdown_flag:
        try:
            msg_type, payload = recv_frame(sock)
        except socket.timeout:
            continue
        except Exception as e:
            print(f"[Peer {peer_id}] receiver closed for {remote_peer_id_or_addr}: {e}")
            rid = remote_peer_id_or_addr if isinstance(remote_peer_id_or_addr, int) else None
            if rid is not None:
                with distribution_state['lock']:
                    distribution_state['outgoing_sockets'].pop(rid, None)
                    distribution_state['incoming_sockets'].pop(rid, None)
            break

        if msg_type == MSG_BITFIELD:
            with distribution_state['lock']:
                total = distribution_state['total_pieces']
                have = parse_bitfield_bytes(payload, total)
                if isinstance(remote_peer_id_or_addr, int):
                    nb_map = distribution_state['neighbor_bitfields'].setdefault(peer_id, {})
                    nb_map[remote_peer_id_or_addr] = set(have)

            print(f"[Peer {peer_id}] <- BITFIELD ({len(payload)} bytes) from "
                f"{remote_peer_id_or_addr} ({len(have)} pieces)")

            if isinstance(remote_peer_id_or_addr, int):
                compute_and_send_interest(peer_id, sock, remote_peer_id_or_addr, have)
                maybe_request_next(peer_id, remote_peer_id_or_addr, sock)
                    
                    
        elif msg_type == MSG_INTERESTED:
            if isinstance(remote_peer_id_or_addr, int):
                with distribution_state['lock']:
                    distribution_state['peer_interest_in_me'][peer_id].add(remote_peer_id_or_addr)
                print(f"[Peer {peer_id}] <- INTERESTED from {remote_peer_id_or_addr}")    
                log_peer(peer_id, f"Peer {peer_id} received the 'interested' message from Peer {remote_peer_id_or_addr}.")

        elif msg_type == MSG_NOTINTERESTED:
            if isinstance(remote_peer_id_or_addr, int):
                with distribution_state['lock']:
                    distribution_state['peer_interest_in_me'][peer_id].discard(remote_peer_id_or_addr)
                print(f"[Peer {peer_id}] <- NOT_INTERESTED from {remote_peer_id_or_addr}")
                log_peer(peer_id, f"Peer {peer_id} received the 'not interested' message from Peer {remote_peer_id_or_addr}.")

        elif msg_type == MSG_CHOKE:
            if isinstance(remote_peer_id_or_addr, int):
                with distribution_state['lock']:
                    distribution_state['choked_by'][peer_id].add(remote_peer_id_or_addr)
            print(f"[Peer {peer_id}] <- CHOKE from {remote_peer_id_or_addr}")
            log_peer(peer_id, f"Peer {peer_id} is choked by Peer {remote_peer_id_or_addr}.")


        elif msg_type == MSG_UNCHOKE:
            if isinstance(remote_peer_id_or_addr, int):
                with distribution_state['lock']:
                    distribution_state['choked_by'][peer_id].discard(remote_peer_id_or_addr)
            print(f"[Peer {peer_id}] <- UNCHOKE from {remote_peer_id_or_addr}")
            log_peer(peer_id, f"Peer {peer_id} is unchoked by Peer {remote_peer_id_or_addr}.")
            maybe_request_next(peer_id, remote_peer_id_or_addr, sock)
            
            
        elif msg_type == MSG_HAVE:
            if len(payload) != 4:
                return
            piece_index = int.from_bytes(payload, 'big', signed=False)
            if isinstance(remote_peer_id_or_addr, int):
                with distribution_state['lock']:
                    nb = distribution_state['neighbor_bitfields'].setdefault(peer_id, {})
                    have_set = nb.setdefault(remote_peer_id_or_addr, set())
                    pre = len(have_set)
                    have_set.add(piece_index)
                print(f"[Peer {peer_id}] <- HAVE {piece_index} from {remote_peer_id_or_addr}")
                log_peer(peer_id, f"Peer {peer_id} received the 'have' message from Peer {remote_peer_id_or_addr} " f"for the piece {piece_index}.")


                print(f"[Peer {peer_id}] <- HAVE {piece_index} from {remote_peer_id_or_addr}")
                compute_and_send_interest(peer_id, sock, remote_peer_id_or_addr, have_set)
                maybe_request_next(peer_id, remote_peer_id_or_addr, sock)
        
        elif msg_type == MSG_REQUEST:
            if len(payload) != 4:
                return
            piece_index = int.from_bytes(payload, 'big', signed=False)
            src = remote_peer_id_or_addr if isinstance(remote_peer_id_or_addr, int) else None
            allowed = False
            with distribution_state['lock']:
                if src is not None:
                    i_unchoke = src in distribution_state['unchoked_out'].get(peer_id, set())
                    i_have = piece_index in distribution_state['peer_pieces'].get(peer_id, set())
                    allowed = i_unchoke and i_have
                    data = distribution_state['pieces'][piece_index] if i_have else b''
            if allowed:
                payload_piece = piece_index.to_bytes(4, 'big') + data
                send_frame(sock, MSG_PIECE, payload_piece)
                print(f"[Peer {peer_id}] -> PIECE {piece_index} to {src} ({len(data)} bytes)")
            else:
                pass

        elif msg_type == MSG_PIECE:
            if len(payload) < 4:
                return
            piece_index = int.from_bytes(payload[:4], 'big', signed=False)
            data = payload[4:]
            with distribution_state['lock']:
                if piece_index not in distribution_state['peer_pieces'].get(peer_id, set()):
                    distribution_state['peer_pieces'][peer_id].add(piece_index)
                nb = remote_peer_id_or_addr if isinstance(remote_peer_id_or_addr, int) else None
                if nb is not None:
                    infl = distribution_state['inflight'][peer_id]
                    if nb in infl:
                        infl.pop(nb, None)
                num_pieces = len(distribution_state['peer_pieces'][peer_id])
                
            print(f"[Peer {peer_id}] <- PIECE {piece_index} ({len(data)} bytes) from {remote_peer_id_or_addr}")
            log_peer( peer_id, f"Peer {peer_id} has downloaded the piece {piece_index} from Peer {remote_peer_id_or_addr}. " f"Now the number of pieces it has is {num_pieces}.")

            broadcast_have(peer_id, piece_index, outgoing_connections, incoming_connections)

            if isinstance(remote_peer_id_or_addr, int):
                now = time.time()
                win = int(common_config.get('UnchokingInterval', 5))
                dq = distribution_state['download_hist'][peer_id][remote_peer_id_or_addr]
                dq.append((now, len(data)))

                cutoff = now - win
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                maybe_request_next(peer_id, remote_peer_id_or_addr, sock)

            evaluate_completion(peer_id)

        
        else:
            pass

# testing flag for dev runs
TESTING = False  # toggle flag

# shutdown flag
shutdown_flag = False

distribution_state = {
    'pieces': [],
    'total_pieces': 0,
    'file_name': '',
    'original_bytes': b'',
    'peer_paths': {},
    'peer_pieces': {},
    'peer_completed': set(),
    'written': set(),
    'seed_assignments': {},
    'leecher_to_seed': {},
    'seed_piece_plan': {},
    'seed_ids': set(),
    'peer_roles': {},
    'lock': threading.Lock(),
    'neighbor_bitfields': {},
    'am_interested_in': {},
    'peer_interest_in_me': {},
    'unchoked_out': {},
    'choked_by': {},
    'download_bytes': {},
    'neighbor_bitfields': {},
    'inflight': {}   
    
}

# --- Logging helpers  ---

log_locks = defaultdict(threading.Lock)

def init_peer_log(peer_id: int):
    """
    Create / truncate the log file for this peer.
    File name: log_peer_<peer_id>.log in the current working directory.
    """
    filename = f"log_peer_{peer_id}.log"
    with log_locks[peer_id]:
        with open(filename, "w", encoding="utf-8") as f:
            pass

def log_peer(peer_id: int, message: str):
    """
    Append a timestamped line to this peer's log file.
    """
    filename = f"log_peer_{peer_id}.log"
    ts = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    line = f"{ts}: {message}\n"
    with log_locks[peer_id]:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(line)


def signal_handler(sig, frame):
    """handle ctrl+c"""
    global shutdown_flag
    print("\n\n" + "="*60)
    print("Ctrl+C detected! Shutting down all peers...")
    print("="*60)
    shutdown_flag = True
    sys.exit(0)

# register signal handler
signal.signal(signal.SIGINT, signal_handler)

def read_common_config(config_dir: Path, filename: str | None = None):
    if filename is None:
        filename = config_dir / "Common.cfg"
    else:
        filename = Path(filename)

    config = {}
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) == 2:
                key, value = parts
                config[key] = int(value) if value.isdigit() else value
    return config


def read_peer_info(config_dir: Path, filename: str | None = None):
    if filename is None:
        filename = config_dir / "PeerInfo.cfg"
    else:
        filename = Path(filename)

    peers = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) != 4:
                continue
            peer_id_str, host, port_str, has_file_flag = parts
            peer = {
                "peer_id": int(peer_id_str),
                "hostname": host,
                "port": int(port_str),
                "has_file": has_file_flag == "1",
                "directory": config_dir / peer_id_str,
            }
            peers.append(peer)

    # Optional local override (see next section)
    if TESTING:
        print("\n" + "!" * 60)
        print("TESTING mode: overriding host/port to localhost:6000+peer_id")
        ids = [p["peer_id"] for p in peers]
        min_peer_id = min(ids)
        for peer in peers:
            peer["hostname"] = "127.0.0.1"
            peer["port"] = 6000 + peer["peer_id"]
            peer["directory"] = config_dir / str(peer["peer_id"])

    return peers




def assign_leechers_to_seeds(peers):
    """balance leechers across seeds"""
    seeds = sorted(
        [peer for peer in peers if peer['has_file']],
        key=lambda peer: peer['peer_id']
    )
    leechers = sorted(
        [peer for peer in peers if not peer['has_file']],
        key=lambda peer: peer['peer_id']
    )

    seed_assignments = {seed['peer_id']: [] for seed in seeds}
    leecher_to_seed = {}
    fairness_summary = {
        'total_leechers': len(leechers),
        'seed_ids': [seed['peer_id'] for seed in seeds],
        'baseline_leechers_per_seed': 0,
        'remainder': 0,
        'per_seed': {}
    }

    if not seeds or not leechers:
        for seed in seeds:
            fairness_summary['per_seed'][seed['peer_id']] = 0
        return seed_assignments, leecher_to_seed, fairness_summary

    base = len(leechers) // len(seeds)
    remainder = len(leechers) % len(seeds)
    fairness_summary['baseline_leechers_per_seed'] = base
    fairness_summary['remainder'] = remainder

    index = 0
    for position, seed in enumerate(seeds):
        count = base + (1 if position < remainder else 0)
        assigned_leechers = leechers[index:index + count]
        seed_assignments[seed['peer_id']] = assigned_leechers
        fairness_summary['per_seed'][seed['peer_id']] = len(assigned_leechers)

        for leecher in assigned_leechers:
            leecher_to_seed[leecher['peer_id']] = seed['peer_id']

        index += count

    return seed_assignments, leecher_to_seed, fairness_summary


def plan_piece_distribution(peers, common_config):
    """balance pieces across seeds"""
    seeds = sorted(
        [peer for peer in peers if peer['has_file']],
        key=lambda peer: peer['peer_id']
    )

    assignments = {seed['peer_id']: [] for seed in seeds}
    summary = {
        'total_pieces': 0,
        'baseline_pieces_per_seed': 0,
        'remainder': 0,
        'per_seed': {}
    }

    if not seeds:
        return assignments, summary

    file_size = common_config.get('FileSize', 0) or 0
    piece_size = common_config.get('PieceSize', 0) or 0

    if piece_size <= 0:
        return assignments, summary

    total_pieces = (file_size + piece_size - 1) // piece_size if file_size > 0 else 0
    summary['total_pieces'] = total_pieces

    if total_pieces == 0:
        for seed in seeds:
            summary['per_seed'][seed['peer_id']] = 0
        return assignments, summary

    base = total_pieces // len(seeds)
    remainder = total_pieces % len(seeds)
    summary['baseline_pieces_per_seed'] = base
    summary['remainder'] = remainder

    next_piece = 0
    for position, seed in enumerate(seeds):
        count = base + (1 if position < remainder else 0)
        pieces = list(range(next_piece, next_piece + count))
        assignments[seed['peer_id']] = pieces
        summary['per_seed'][seed['peer_id']] = len(pieces)
        next_piece += count

    return assignments, summary


def load_file_pieces(peers, common_config, config_dir: Path):
    """read source file into pieces"""
    file_name = common_config.get('FileName', 'thefile')
    piece_size = common_config.get('PieceSize', 0) or 0

    source_path = None
    for peer in peers:
        if peer['has_file']:
            candidate = peer.get('directory')
            if candidate is not None:
                candidate_path = candidate / file_name
            else:
                candidate_path = config_dir / str(peer["peer_id"]) / file_name
            if candidate_path.exists():
                source_path = candidate_path
                break

    file_bytes = b''
    if source_path and source_path.exists():
        file_bytes = source_path.read_bytes()
        print(f"\nLoaded source file from {source_path} ({len(file_bytes)} bytes)")
    else:
        print("\nWarning: Unable to locate source file in seed directories. Starting with empty data set.")

    if piece_size > 0:
        pieces = [
            file_bytes[offset:offset + piece_size]
            for offset in range(0, len(file_bytes), piece_size)
        ]
        if not pieces:
            pieces = [b'']
    else:
        pieces = [file_bytes]

    return pieces, file_name, file_bytes


def initialize_distribution_state(peers, pieces, file_name, original_bytes, seed_assignments, leecher_to_seed, seed_piece_plan):
    """init distribution state"""
    total_pieces = len(pieces)

    with distribution_state['lock']:
        distribution_state['pieces'] = pieces
        distribution_state['total_pieces'] = total_pieces
        distribution_state['file_name'] = file_name
        distribution_state['original_bytes'] = original_bytes
        distribution_state['peer_paths'] = {}
        distribution_state['peer_pieces'] = {}
        distribution_state['peer_completed'] = set()
        distribution_state['written'] = set()
        distribution_state['seed_assignments'] = {
            seed_id: {leecher['peer_id'] for leecher in leechers}
            for seed_id, leechers in seed_assignments.items()
        }
        distribution_state['leecher_to_seed'] = dict(leecher_to_seed)
        distribution_state['seed_piece_plan'] = {
            seed_id: set(piece_list)
            for seed_id, piece_list in seed_piece_plan.items()
        }
        distribution_state['seed_ids'] = {
            peer['peer_id'] for peer in peers if peer['has_file']
        }
        distribution_state['peer_roles'] = {
            peer['peer_id']: peer['has_file'] for peer in peers
        }
        distribution_state['am_interested_in'] = {
            peer['peer_id']: set() for peer in peers
        }
        distribution_state['peer_interest_in_me'] = {
            peer['peer_id']: set() for peer in peers
        }
        distribution_state['neighbor_bitfields'] = {}
        for peer in peers:
            distribution_state['neighbor_bitfields'][peer['peer_id']] = {}
        distribution_state['unchoked_out'] = {
            peer['peer_id']: set() for peer in peers
        }
        distribution_state['choked_by'] = {
            peer['peer_id']: set() for peer in peers
        }
        distribution_state['download_bytes'] = {
            peer['peer_id']: {}   for peer in peers
        }
        for me in distribution_state['choked_by'].keys():
            others = {p['peer_id'] for p in peers if p['peer_id'] != me}
            distribution_state['choked_by'][me] = others
        base_dir = CONFIG_DIR
        for peer in peers:
            peer_id = peer['peer_id']
            peer_dir = peer.get('directory') or (base_dir / str(peer_id))
            peer_dir.mkdir(parents=True, exist_ok=True)
            distribution_state['peer_paths'][peer_id] = peer_dir

            if peer['has_file']:
                distribution_state['peer_pieces'][peer_id] = set(range(total_pieces))
            else:
                distribution_state['peer_pieces'][peer_id] = set()
        distribution_state['neighbor_bitfields'] = {
            p['peer_id']: {} for p in peers
        }
        distribution_state['inflight'] = {
            p['peer_id']: {} for p in peers
        }

def write_completed_file(peer_id):
    """write assembled file once"""
    with distribution_state['lock']:
        if peer_id in distribution_state['written']:
            return

        pieces = distribution_state['pieces']
        file_name = distribution_state['file_name']
        peer_path = distribution_state['peer_paths'].get(peer_id)

    if peer_path is None:
        return

    peer_path.mkdir(parents=True, exist_ok=True)
    output_path = peer_path / file_name
    with open(output_path, 'wb') as f:
        for chunk in pieces:
            f.write(chunk)

    with distribution_state['lock']:
        distribution_state['written'].add(peer_id)

    print(f"[Peer {peer_id}] Assembled file written to {output_path}")


def evaluate_completion(peer_id):
    """check peer and swarm done"""
    global shutdown_flag
    write_targets = []
    swarm_complete = False

    with distribution_state['lock']:
        total_pieces = distribution_state['total_pieces']
        peer_pieces = distribution_state['peer_pieces'].get(peer_id, set())

        if total_pieces == len(peer_pieces):
            if peer_id not in distribution_state['peer_completed']:
                distribution_state['peer_completed'].add(peer_id)
                write_targets.append(peer_id)

        peer_count = len(distribution_state['peer_pieces'])
        if peer_count and len(distribution_state['peer_completed']) == peer_count:
            swarm_complete = True

    for pid in write_targets:
        log_peer(pid, f"Peer {pid} has downloaded the complete file.")
        write_completed_file(pid)

    if swarm_complete and not shutdown_flag:
        print("\n" + "="*60)
        print("All peers now have the complete dataset. Initiating coordinated shutdown.")
        print("="*60)
        shutdown_flag = True


def reset_project_files(peers, file_name, config_dir: Path):
    """restore files so only seeds keep data"""
    base_dir = config_dir
    removed = []
    restored = []

    with distribution_state['lock']:
        original_bytes = distribution_state.get('original_bytes', b'')
        total_pieces = distribution_state.get('total_pieces', 0)

    for peer in peers:
        peer_dir = peer.get('directory') or (base_dir / str(peer['peer_id']))
        target = peer_dir / file_name

        if peer['has_file']:
            if original_bytes:
                peer_dir.mkdir(parents=True, exist_ok=True)
                try:
                    target.write_bytes(original_bytes)
                except Exception as exc:
                    print(f"[Reset] failed to write seed file for {peer['peer_id']}: {exc}")
            restored.append(peer['peer_id'])
        else:
            if target.exists():
                try:
                    target.unlink()
                    removed.append(peer['peer_id'])
                except Exception as exc:
                    print(f"[Reset] failed to remove file for {peer['peer_id']}: {exc}")

    with distribution_state['lock']:
        seed_ids = {peer['peer_id'] for peer in peers if peer['has_file']}
        for peer in peers:
            peer_id = peer['peer_id']
            if peer_id in seed_ids:
                distribution_state['peer_pieces'][peer_id] = set(range(total_pieces))
            else:
                distribution_state['peer_pieces'][peer_id] = set()
        distribution_state['peer_completed'] = set()
        distribution_state['written'] = set()

    print("\n" + "="*60)
    print("Project files reset to initial distribution.")
    if restored:
        print(f"  Seeds restored: {sorted(restored)}")
    if removed:
        print(f"  Leecher files removed: {sorted(removed)}")
    print("="*60)

def handle_incoming_connection(peer_id, client_socket, address, outgoing_connections, incoming_connections):
    """handle incoming peer socket"""
    print(f"[Peer {peer_id}] Handling connection from {address}")
    
    try:
       # --- Handshake ---
        their_id = recv_handshake(client_socket)
        send_handshake(client_socket, peer_id)
        print(f"[Peer {peer_id}] Handshake OK with {their_id} from {address}")
        with incoming_connections['lock']:
            incoming_connections['sockets'][their_id] = client_socket
        with distribution_state['lock']:
            distribution_state['incoming_sockets'].setdefault(peer_id, {})[their_id] = client_socket
            distribution_state['neighbor_locks'].setdefault(their_id, threading.Lock())
        print(f"[Peer {peer_id}] mapped IN  -> {their_id}")

        log_peer(peer_id, f"Peer {peer_id} is connected from Peer {their_id}.")
        
        # --- Send our bitfield right after handshake ---
        with distribution_state['lock']:
            my_bits = build_bitfield_bytes(
                distribution_state['peer_pieces'][peer_id],
                distribution_state['total_pieces']
            )
        send_frame(client_socket, MSG_BITFIELD, my_bits)

        # --- Enter framed-message receive loop ---
        receiver_loop(peer_id, client_socket, their_id, outgoing_connections, incoming_connections)
                
    except Exception as e:
        if not shutdown_flag:
            print(f"[Peer {peer_id}] Error handling connection: {e}")
    finally:
        try:
            client_socket.close()
        except:
            pass

def accept_connections(peer_id, server_socket, outgoing_connections, incoming_connections):
    """accept peer sockets"""
    print(f"[Peer {peer_id}] Ready to accept connections")
    
    server_socket.settimeout(1.0)
    
    while not shutdown_flag:
        try:
            client_socket, address = server_socket.accept()
            print(f"[Peer {peer_id}] Accepted connection from {address}")
            
            # count incoming connection
            with incoming_connections['lock']:
                incoming_connections['count'] += 1
            
            # spawn handler thread
            handler_thread = threading.Thread(
                target=handle_incoming_connection,
                args=(peer_id, client_socket, address, outgoing_connections, incoming_connections),
                daemon=True
            )
            handler_thread.start()
            
        except socket.timeout:
            continue
        except Exception as e:
            if not shutdown_flag:
                print(f"[Peer {peer_id}] Error accepting connection: {e}")


def establish_connections(peer_id, connection_targets, outgoing_connections, incoming_connections):
    """dial planned peers with retries"""
    targets = {
        peer['peer_id']: peer
        for peer in connection_targets
        if peer['peer_id'] != peer_id
    }
    last_failure_log = {}

    while targets and not shutdown_flag:
        for target_id in list(targets.keys()):
            if shutdown_flag:
                break

            if target_id in outgoing_connections:
                targets.pop(target_id, None)
                continue

            target_peer = targets[target_id]
            client = None

            try:
                print(f"[Peer {peer_id}] Connecting to peer {target_id} at "
                      f"{target_peer['hostname']}:{target_peer['port']}")

                client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                client.settimeout(5.0)
                client.connect((target_peer['hostname'], target_peer['port']))

                # --- handshake both ways ---
                send_handshake(client, peer_id)
                their_id = recv_handshake(client)
                print(f"[Peer {peer_id}] Handshake OK with {their_id}")
                log_peer(peer_id, f"Peer {peer_id} makes a connection to Peer {their_id}.")
                with distribution_state['lock']:
                    distribution_state['outgoing_sockets'].setdefault(peer_id, {})[their_id] = client
                    distribution_state['neighbor_locks'].setdefault(their_id, threading.Lock())
                print(f"[Peer {peer_id}] mapped OUT -> {their_id}")

                # --- send our bitfield immediately ---
                with distribution_state['lock']:
                    my_bits = build_bitfield_bytes(
                        distribution_state['peer_pieces'][peer_id],
                        distribution_state['total_pieces']
                    )
                send_frame(client, MSG_BITFIELD, my_bits)

                client.settimeout(None)
                outgoing_connections[target_id] = client
                targets.pop(target_id, None)

                # --- start a receiver for outgoing connection ---
                threading.Thread(
                    target=receiver_loop,
                    args=(peer_id, client, target_id, outgoing_connections, incoming_connections),
                    daemon=True
                ).start()

            except Exception as e:
                if client is not None:
                    try:
                        client.close()
                    except Exception:
                        pass

                now = time.time()
                last_logged = last_failure_log.get(target_id, 0)
                if now - last_logged > 5:
                    print(f"[Peer {peer_id}] Failed to connect to peer {target_id}: {e} "
                          f"(retrying)")
                    last_failure_log[target_id] = now

        if targets and not shutdown_flag:
            time.sleep(1)


def pick_next_piece_to_request(my_id, neighbor_id):
    with distribution_state['lock']:
        my_have  = distribution_state['peer_pieces'].get(my_id, set())
        inflight_vals = distribution_state['inflight'].get(my_id, {}).values()
        inflight_pieces = {piece for (piece, _) in inflight_vals}
        nb_map   = distribution_state['neighbor_bitfields'].get(my_id, {})

        avail_counts = {}
        for nb, have in nb_map.items():
            for idx in have:
                if idx not in my_have and idx not in inflight_pieces:
                    avail_counts[idx] = avail_counts.get(idx, 0) + 1

        candidates = [i for i in nb_map.get(neighbor_id, set())
                      if i not in my_have and i not in inflight_pieces]
        if not candidates:
            return None

        candidates.sort(key=lambda i: (avail_counts.get(i, 0), i))
        return candidates[0]

def send_request(my_id, neighbor_id, sock, piece_index):
    payload = piece_index.to_bytes(4, 'big', signed=False)
    send_frame(sock, MSG_REQUEST, payload)
    with distribution_state['lock']:
        distribution_state['inflight'][my_id][neighbor_id] = (piece_index, time.time())
    print(f"[Peer {my_id}] -> REQUEST piece {piece_index} to {neighbor_id}")

def maybe_request_next(my_id, neighbor_id, sock):
    with distribution_state['lock']:
        interested = neighbor_id in distribution_state['am_interested_in'].get(my_id, set())
        unchoked_by_neighbor = neighbor_id not in distribution_state['choked_by'].get(my_id, set())
        has_inflight = neighbor_id in distribution_state['inflight'].get(my_id, {})
    if not (interested and unchoked_by_neighbor) or has_inflight:
        return
    nxt = pick_next_piece_to_request(my_id, neighbor_id)
    if nxt is not None:
        send_request(my_id, neighbor_id, sock, nxt)

def broadcast_have(my_id, piece_index, outgoing_connections, incoming_connections):
    payload = piece_index.to_bytes(4, 'big', signed=False)
    with incoming_connections['lock']:
        inbound = dict(incoming_connections.get('sockets', {}))
    all_socks = dict(outgoing_connections); all_socks.update(inbound)

    for nb_id, sock in list(all_socks.items()):
        try:
            send_frame(sock, MSG_HAVE, payload)
        except Exception:
            pass
    print(f"[Peer {my_id}] -> HAVE {piece_index} (broadcast)")

def download_rate(my_id, neighbor_id, window_secs) -> float:
    dq = distribution_state['download_hist'][my_id][neighbor_id]
    now = time.time()
    cutoff = now - window_secs
    total = 0
    for t, b in dq:
        if t >= cutoff:
            total += b
    elapsed = max(1e-6, now - cutoff)
    return total / elapsed


def peer_process(peer_info, all_peers, common_config, seed_assignments,
                 leecher_to_seed, leecher_fairness,
                 seed_piece_plan, piece_plan_summary):
    """run peer workflow in thread"""
    peer_id = peer_info['peer_id']
    hostname = peer_info['hostname']
    port = peer_info['port']
    has_file = peer_info['has_file']
    
    init_peer_log(peer_id)
    log_peer(peer_id, f"Peer {peer_id} started on {hostname}:{port}. Has file: {bool(has_file)}.")
    
    print(f"\n[Peer {peer_id}] Starting on {hostname}:{port}, has_file={has_file}")
    
    # track incoming state and outgoing connections
    incoming_connections = {
        'count': 0,
        'lock': threading.Lock(),
        'sockets': {}
    }
    outgoing_connections = {}
    
    interested_from = set()
    unchoked_upload = set()

    REQUEST_TIMEOUT = 5.0
    MAX_INFLIGHT_PER_NEIGHBOR = 2

    with distribution_state['lock']:
        distribution_state.setdefault('inflight', {}).setdefault(peer_id, {}) 
    
    # step 1 start server
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', port))
        server.listen(10)
        
        print(f"[Peer {peer_id}] Server listening on port {port}")
    except Exception as e:
        print(f"[Peer {peer_id}] Failed to start server: {e}")
        return
    
    # spin up accept thread
    accept_thread = threading.Thread(
        target=accept_connections,
        args=(peer_id, server, outgoing_connections, incoming_connections),
        daemon=True
    )
    accept_thread.start()
    
    # brief server delay
    time.sleep(1)
    
    assigned_peer_ids = []
    assigned_piece_ids = []
    propagation_message = "participating in swarm"
    assigned_seed_id = None
    
    # step 2 dial mesh
    

    mesh_targets = [
        peer
        for peer in all_peers
        if peer['peer_id'] != peer_id and peer['peer_id'] < peer_id
    ]
    connector_thread = threading.Thread(
        target=establish_connections,
        args=(peer_id, mesh_targets, outgoing_connections, incoming_connections),
        daemon=True
    )
    connector_thread.start()
    
    if has_file:
        assigned_leechers = seed_assignments.get(peer_id, [])
        assigned_ids = [peer['peer_id'] for peer in assigned_leechers]
        baseline_leechers = leecher_fairness.get('baseline_leechers_per_seed', 0)
        leecher_count = len(assigned_ids)
        extra_leecher = leecher_count > baseline_leechers

        assigned_pieces = seed_piece_plan.get(peer_id, [])
        baseline_pieces = piece_plan_summary.get('baseline_pieces_per_seed', 0)
        extra_piece = len(assigned_pieces) > baseline_pieces

        print(f"[Peer {peer_id}] Assigned leechers: {assigned_ids} "
              f"(baseline {baseline_leechers}{' +1' if extra_leecher else ''})")
        print(f"[Peer {peer_id}] Piece responsibility: {assigned_pieces} "
              f"(baseline {baseline_pieces}{' +1' if extra_piece else ''})")
        assigned_peer_ids = assigned_ids
        assigned_piece_ids = assigned_pieces
        propagation_message = (f"seeding {len(assigned_piece_ids)} pieces to "
                               f"{len(assigned_peer_ids)} leechers")
    else:
        seed_id = leecher_to_seed.get(peer_id)
        assigned_ids = [seed_id] if seed_id is not None else []
        seed_pieces = seed_piece_plan.get(seed_id, []) if seed_id is not None else []

        print(f"[Peer {peer_id}] Assigned seed: {assigned_ids}")
        if seed_pieces:
            print(f"[Peer {peer_id}] Will request initial pieces {seed_pieces} from seed {seed_id}")
        else:
            print(f"[Peer {peer_id}] Awaiting propagated pieces from peers")
        assigned_peer_ids = assigned_ids
        assigned_piece_ids = seed_pieces
        assigned_seed_id = seed_id
        if seed_id is not None and seed_pieces:
            propagation_message = (f"downloading {len(seed_pieces)} pieces from seed {seed_id} "
                                   "and propagating to neighbors")
        elif seed_id is not None:
            propagation_message = f"coordinating with seed {seed_id} for upcoming pieces"
        else:
            propagation_message = "waiting for seed assignment"
    
    # step 3 share pieces
    total_expected_peers = len(all_peers) - 1  # exclude self
    assigned_leechers = seed_assignments.get(peer_id, []) if has_file else []

    print(f"[Peer {peer_id}] Setup complete. Running...")
    print(f"[Peer {peer_id}] Outgoing: {list(outgoing_connections.keys())}")
    
    k = int(common_config.get('NumberOfPreferredNeighbors', 3))
    p = int(common_config.get('UnchokingInterval', 5))
    m = int(common_config.get('OptimisticUnchokingInterval', 10))

    threading.Thread(target=choke_scheduler,
                    args=(peer_id, k, p),
                    daemon=True).start()

    threading.Thread(target=optimistic_unchoke_runner,
                    args=(peer_id, m),
                    daemon=True).start()

    threading.Thread(
        target=request_watchdog,
        args=(peer_id, int(common_config.get('RequestTimeout', 15))),
        daemon=True
    ).start()



    last_status_time = time.time()

    evaluate_completion(peer_id)

    try:
        while not shutdown_flag:
            time.sleep(1)
            if shutdown_flag:
                break

            evaluate_completion(peer_id)

            now = time.time()
            if now - last_status_time >= 5 and not shutdown_flag:
                with incoming_connections['lock']:
                    incoming = incoming_connections['count']
                outgoing = len(outgoing_connections)
                total = incoming + outgoing

                with distribution_state['lock']:
                    current_piece_count = len(distribution_state['peer_pieces'].get(peer_id, set()))
                    total_piece_count = distribution_state['total_pieces']

                remaining = max(total_piece_count - current_piece_count, 0)

                if has_file:
                    propagation_message = (
                        f"seeding {len(assigned_piece_ids)} designated pieces; "
                        f"holding {current_piece_count}/{total_piece_count}"
                    )
                else:
                    if assigned_seed_id is not None:
                        propagation_message = (
                            f"syncing with seed {assigned_seed_id}; missing {remaining} pieces"
                        )
                    else:
                        propagation_message = f"swarm propagation; missing {remaining} pieces"

                print(f"[Peer {peer_id}] Connections: {total}/{total_expected_peers} "
                      f"(outgoing: {outgoing}, incoming: {incoming}) | {propagation_message}")
                last_status_time = now
    except Exception as e:
        pass
    finally:
        print(f"[Peer {peer_id}] Shutting down...")
        
        # clean up sockets
        for conn in outgoing_connections.values():
            try:
                conn.close()
            except:
                pass
        try:
            server.close()
        except:
            pass

def get_socket_for(neighbor_id, outgoing_connections, incoming_connections):
    s = outgoing_connections.get(neighbor_id)
    if not s:
        s = incoming_connections.get('sockets', {}).get(neighbor_id)
    return s

def sock_for(my_id, neighbor_id):
    """
    Return the socket my_id should use to talk to neighbor_id,
    using either the outgoing or incoming side.
    """
    s = distribution_state['outgoing_sockets'].get(my_id, {}).get(neighbor_id)
    if s is None:
        s = distribution_state['incoming_sockets'].get(my_id, {}).get(neighbor_id)
    return s


def choke_scheduler(peer_id, k, interval):
    
    while not shutdown_flag:
        time.sleep(interval)

 
        with distribution_state['lock']:
            outgoing_neighbors = set(distribution_state['outgoing_sockets'].get(peer_id, {}).keys())
            incoming_neighbors = set(distribution_state['incoming_sockets'].get(peer_id, {}).keys())
            neighbors = outgoing_neighbors | incoming_neighbors

            interested = distribution_state['peer_interest_in_me'].get(peer_id, set()) & neighbors
            unchoked_now = set(distribution_state['unchoked_out'].get(peer_id, set()))
            optimistic = distribution_state['optimistic_peer'].get(peer_id)


        if not neighbors:
            continue

        scored = []
        for neighbor_id in interested:
            r = download_rate(peer_id, neighbor_id, interval)
            scored.append((-r, random.random(), neighbor_id))
        scored.sort()
        preferred = {neighbor_id for _, _, neighbor_id in scored[:k]}

        with distribution_state['lock']:
            distribution_state['preferred_out'][peer_id] = preferred

        neighbor_list_str = ", ".join(str(pid) for pid in sorted(preferred))
        log_peer(peer_id, f"Peer {peer_id} has the preferred neighbors [{neighbor_list_str}].")


        allow = set(preferred)
        if optimistic is not None:
            allow.add(optimistic)

        to_unchoke = allow - unchoked_now
        to_choke   = unchoked_now - allow

        for neighbor_id in to_unchoke:
            s = sock_for(peer_id, neighbor_id)
            if not s:
                print(f"[Peer {peer_id}] WARN: no socket for {neighbor_id} when UNCHOKE")
                continue
            send_frame(s, MSG_UNCHOKE, b'')
            with distribution_state['lock']:
                distribution_state['unchoked_out'].setdefault(peer_id, set()).add(neighbor_id)
            print(f"[Peer {peer_id}] -> UNCHOKE to {neighbor_id}")
            maybe_request_next(peer_id, neighbor_id, s)

        for neighbor_id in to_choke:
            s = sock_for(peer_id, neighbor_id)
            if not s:
                print(f"[Peer {peer_id}] WARN: no socket for {neighbor_id} when CHOKE")
                continue
            send_frame(s, MSG_CHOKE, b'')
            with distribution_state['lock']:
                distribution_state['unchoked_out'].setdefault(peer_id, set()).discard(neighbor_id)
            print(f"[Peer {peer_id}] -> CHOKE to {neighbor_id}")

def optimistic_unchoke_runner(peer_id, interval_m):

    last = None
    while not shutdown_flag:
        time.sleep(interval_m)

        with distribution_state['lock']:
            outgoing_neighbors = set(distribution_state['outgoing_sockets'].get(peer_id, {}).keys())
            incoming_neighbors = set(distribution_state['incoming_sockets'].get(peer_id, {}).keys())
            neighbors = outgoing_neighbors | incoming_neighbors

            interested  = distribution_state['peer_interest_in_me'].get(peer_id, set()) & neighbors
            preferred   = set(distribution_state['preferred_out'].get(peer_id, set()))
            unchoked    = set(distribution_state['unchoked_out'].get(peer_id, set()))

        candidates = list(interested - preferred - unchoked)
        if not candidates:
            with distribution_state['lock']:
                distribution_state['optimistic_peer'][peer_id] = None
            continue

        pick = random.choice(candidates)
        if pick == last:
            continue 

        if last is not None and last not in preferred:
            s_old = sock_for(peer_id, last)
            if s_old:
                send_frame(s_old, MSG_CHOKE, b'')
                with distribution_state['lock']:
                    distribution_state['unchoked_out'].setdefault(peer_id, set()).discard(last)
                print(f"[Peer {peer_id}] -> CHOKE (old optimistic) {last}")


        with distribution_state['lock']:
            distribution_state['optimistic_peer'][peer_id] = pick

        log_peer(peer_id, f"Peer {peer_id} has the optimistically unchoked neighbor [{pick}].")

        s_new = sock_for(peer_id, pick)
        if s_new:
            send_frame(s_new, MSG_UNCHOKE, b'')
            with distribution_state['lock']:
                distribution_state['unchoked_out'].setdefault(peer_id, set()).add(pick)
            print(f"[Peer {peer_id}] -> OPT-UNCHOKE to {pick}")
            maybe_request_next(peer_id, pick, s_new)

        last = pick
   
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("peer_id", nargs="?", type=int, help="run only this peer id")
    parser.add_argument("--config-dir", default=None, help="directory containing Common.cfg, PeerInfo.cfg, and peer folders",)
    parser.add_argument("--no-reset", action="store_true", help="keep files after run")
    parser.add_argument("--reset", action="store_true", help="reset project files then exit")
    args = parser.parse_args()
    
    # read configs
    CONFIG_DIR = find_config_dir(args.config_dir)
    print(f"\nUsing config directory: {CONFIG_DIR}\n")
    
    
    common_config = read_common_config(CONFIG_DIR)
    all_peers = read_peer_info(CONFIG_DIR)
    seed_assignments, leecher_to_seed, leecher_fairness = assign_leechers_to_seeds(all_peers)
    seed_piece_plan, piece_plan_summary = plan_piece_distribution(all_peers, common_config)
    pieces, file_name, original_bytes = load_file_pieces(all_peers, common_config, CONFIG_DIR)
    initialize_distribution_state(
        all_peers,
        pieces,
        file_name,
        original_bytes,
        seed_assignments,
        leecher_to_seed,
        seed_piece_plan
    )
    if args.reset:
        reset_project_files(all_peers, file_name, CONFIG_DIR)
        sys.exit(0)
    
    distribution_state.setdefault('peer_interest_in_me', defaultdict(set))
    distribution_state.setdefault('unchoked_out', defaultdict(set))
    distribution_state.setdefault('optimistic_peer', {})

    distribution_state.setdefault('download_hist', defaultdict(lambda: defaultdict(deque)))

    distribution_state.setdefault('outgoing_sockets', defaultdict(dict))
    distribution_state.setdefault('incoming_sockets', defaultdict(dict))
    distribution_state.setdefault('neighbor_locks', defaultdict(threading.Lock))
    
    distribution_state.setdefault('preferred_out', defaultdict(set))

    print("="*60)
    print("Common Config:")
    pprint(common_config)
    
    print("\n" + "="*60)
    print("Peer Info:")
    peers_to_run = all_peers if args.peer_id is None else [p for p in all_peers if p["peer_id"] == args.peer_id]
    for peer in peers_to_run:
        has_file_str = "SEED" if peer['has_file'] else "LEECHER"
        print(f"  Peer {peer['peer_id']}: {peer['hostname']}:{peer['port']} - {has_file_str}")
    print("="*60)
    
    if leecher_fairness['seed_ids']:
        print("\n" + "="*60)
        print("Seeder Load Agreement:")
        baseline = leecher_fairness['baseline_leechers_per_seed']
        remainder = leecher_fairness['remainder']
        print(f"  Baseline leechers per seed: {baseline}")
        if remainder:
            print(f"  First {remainder} seeds (by peer_id) receive one extra leecher")
        for seed_id in sorted(leecher_fairness['seed_ids']):
            assigned_ids = [peer['peer_id'] for peer in seed_assignments.get(seed_id, [])]
            print(f"  Seed {seed_id}: leechers {assigned_ids}")
        print("="*60)

    if seed_piece_plan:
        print("\n" + "="*60)
        print("Piece Distribution Plan:")
        print(f"  Total pieces: {piece_plan_summary['total_pieces']}")
        baseline_pieces = piece_plan_summary['baseline_pieces_per_seed']
        remainder_pieces = piece_plan_summary['remainder']
        print(f"  Baseline pieces per seed: {baseline_pieces}")
        if remainder_pieces:
            print(f"  First {remainder_pieces} seeds (by peer_id) take one extra piece")
        for seed_id in sorted(seed_piece_plan.keys()):
            pieces = seed_piece_plan[seed_id]
            print(f"  Seed {seed_id}: piece indices {pieces}")
        print("="*60)
    
    # spawn peer threads
    threads = []
    for peer_info in peers_to_run:
        thread = threading.Thread(
            target=peer_process,
            args=(
                peer_info,
                all_peers,
                common_config,
                seed_assignments,
                leecher_to_seed,
                leecher_fairness,
                seed_piece_plan,
                piece_plan_summary
            ),
            daemon=True
        )
        threads.append(thread)
        thread.start()
        
        # small launch delay
        time.sleep(0.5)
    
    print("\n" + "="*60)
    print("All peers started!")
    print("Press Ctrl+C to stop")
    print("="*60)
    
    # keep main thread alive
    try:
        while not shutdown_flag:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        shutdown_flag = True
    
    # wait for cleanup
    time.sleep(2)
    print("All peers stopped.")
    
    if not args.no_reset:
        reset_project_files(all_peers, file_name, CONFIG_DIR)