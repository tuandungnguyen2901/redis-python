import socket
import asyncio
import time
import argparse
import re
from typing import Dict, Tuple, Optional, Set
import base64
from asyncio import StreamReader, StreamWriter

from resp import RESPProtocol
from replication import ReplicationManager
from redis_server import Redis

async def main() -> None:
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--port', type=int, default=6379, help='Port to listen on')
    parser.add_argument('--replicaof', nargs='+', help='Master host and port for replication')
    parser.add_argument('--dir', type=str, default=".", help='Directory for the RDB file')
    parser.add_argument('--dbfilename', type=str, default="dump.rdb", help='Filename for the RDB file')
    parser.add_argument('--heartbeat-interval', type=float, default=5.0, help='Seconds between heartbeats')
    parser.add_argument('--heartbeat-timeout', type=float, default=15.0, help='Seconds before considering master down')
    parser.add_argument('--election-timeout', type=float, default=5.0, help='Seconds for election process')
    parser.add_argument('--priority', type=int, default=100, help='Node priority for elections (higher = more likely to become master)')
    parser.add_argument('--no-cluster', action='store_true', help='Disable cluster features')
    
    args = parser.parse_args()
    
    redis = Redis(args.port, args.dir, args.dbfilename)
    
    # Set cluster configuration
    redis.config.set("heartbeat_interval", args.heartbeat_interval)
    redis.config.set("heartbeat_timeout", args.heartbeat_timeout)
    redis.config.set("election_timeout", args.election_timeout)
    redis.config.set("priority", args.priority)
    redis.config.set("cluster_enabled", not args.no_cluster)
    
    if args.replicaof:
        redis.replication.role = "slave"
        
        # Handle different formats of replicaof argument
        if len(args.replicaof) == 1 and ' ' in args.replicaof[0]:
            # Format: --replicaof "localhost 6379"
            master_host, master_port = args.replicaof[0].split()
        elif len(args.replicaof) == 2:
            # Format: --replicaof localhost 6379
            master_host, master_port = args.replicaof
        else:
            print("Error: Invalid format for --replicaof. Use either --replicaof HOST PORT or --replicaof \"HOST PORT\"")
            return
            
        redis.replication.master_host = master_host
        redis.replication.master_port = int(master_port)
    
    await redis.start()

if __name__ == "__main__":
    asyncio.run(main())
