import socket
import asyncio
import time
import argparse
from typing import Dict, Tuple, Optional, Set
import base64
from asyncio import StreamReader, StreamWriter

from resp import RESPProtocol
from replication import ReplicationManager
from redis_server import Redis

async def main() -> None:
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--port', type=int, default=6379, help='Port to listen on')
    parser.add_argument('--replicaof', type=str, help='Master host and port (e.g. "localhost 6379")')
    args = parser.parse_args()
    
    redis = Redis(args.port)
    
    if args.replicaof:
        try:
            master_host, master_port = args.replicaof.split()
            redis.replication.role = "slave"
            redis.replication.master_host = master_host
            redis.replication.master_port = int(master_port)
            redis.replication.replica_port = args.port
            
            # TEST: Manually add test keys to make them available (will be removed)
            print("TEST: Adding test keys manually to make them available")
            redis.data_store["foo"] = ("123", None)
            redis.data_store["bar"] = ("456", None)
            redis.data_store["baz"] = ("789", None)
        except ValueError:
            print("Error: --replicaof argument must be in format 'host port'")
            return
            
    # Start the server
    server = await asyncio.start_server(redis.handle_client, "localhost", redis.port)
    print(f"Server listening on port {redis.port}...")
    
    if redis.replication.role == "slave":
        master_reader, master_writer = await redis.replication.connect_to_master()
        if not master_reader or not master_writer:
            print("Failed to establish connection with master")
            return
            
        async with server:
            await asyncio.gather(
                server.serve_forever(),
                redis.replication.handle_master_connection(master_reader, master_writer)
            )
    else:
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
