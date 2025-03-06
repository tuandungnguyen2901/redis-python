import socket
import asyncio
import time
import argparse
from typing import Dict, Tuple, Optional

# Store both value and expiry timestamp (in ms since epoch)
data_store: Dict[str, Tuple[str, Optional[int]]] = {}

# Server configuration
server_config = {
    "role": "master",
    "master_host": None,
    "master_port": None,
    "master_replid": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",  # Hardcoded replication ID
    "master_repl_offset": 0  # Starting offset
}

def get_current_time_ms():
    return int(time.time() * 1000)

def is_key_expired(key):
    if key not in data_store:
        return True
    value, expiry = data_store[key]
    if expiry is None:
        return False
    return get_current_time_ms() >= expiry

def format_info_response(section=None):
    """Format INFO response according to RESP protocol"""
    if section == "replication":
        info_lines = [
            f"role:{server_config['role']}",
            f"master_replid:{server_config['master_replid']}",
            f"master_repl_offset:{server_config['master_repl_offset']}"
        ]
        info_str = "\n".join(info_lines)
        return f"${len(info_str)}\r\n{info_str}\r\n"
    return "$-1\r\n"  # Return nil for unknown sections

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print("Connected", addr)
    buffer = ""
    while True:
        try:
            data = await reader.read(1024)
            if not data:
                break
            
            # Append new data to buffer
            buffer += data.decode()
            
            # Process complete commands from buffer
            while '\r\n' in buffer:
                # Find the first complete command
                message, *remaining = buffer.split('\r\n', 1)
                if not message:
                    buffer = remaining[0] if remaining else ""
                    continue
                
                # Parse RESP protocol input
                lines = [message]
                if remaining:
                    lines.extend(remaining[0].split('\r\n'))
                print("Parsed lines:", lines)
                
                # Handle RESP array format
                command = None
                args = []
                
                # Check if it's an array command
                if lines[0].startswith('*'):
                    try:
                        array_length = int(lines[0][1:])
                        expected_lines = 2 + (2 * array_length)  # Array count + (length + value) pairs
                        
                        # Check if we have all the lines we need
                        if len(lines) < expected_lines:
                            break  # Wait for more data
                        
                        item_index = 1
                        for i in range(array_length):
                            if lines[item_index].startswith('$'):
                                item_index += 1
                                if command is None:
                                    command = lines[item_index].upper()
                                    print(f"Command detected: {command}")
                                else:
                                    args.append(lines[item_index])
                                    print(f"Argument detected: {lines[item_index]}")
                                item_index += 1
                        
                        # Remove processed command from buffer
                        buffer = '\r\n'.join(lines[expected_lines:])
                    except (ValueError, IndexError) as e:
                        print(f"Error parsing array command: {e}")
                        writer.write(b"-ERR protocol error\r\n")
                        await writer.drain()
                        continue
                else:
                    # Simple command parsing (fallback)
                    parts = message.strip().split()
                    command = parts[0].upper() if parts else ""
                    args = parts[1:] if len(parts) > 1 else []
                    buffer = remaining[0] if remaining else ""
                
                print(f"Processed command: {command}, args: {args}")
                
                # Execute the command
                if command == "PING":
                    writer.write(b"+PONG\r\n")
                elif command == "ECHO" and args:
                    echo_arg = args[0]
                    resp = f"${len(echo_arg)}\r\n{echo_arg}\r\n"
                    writer.write(resp.encode())
                elif command == "REPLCONF":
                    print(f"Received REPLCONF with args: {args}")
                    writer.write(b"+OK\r\n")
                elif command == "INFO":
                    # Handle INFO command with optional section argument
                    section = args[0].lower() if args else None
                    resp = format_info_response(section)
                    writer.write(resp.encode())
                elif command == "SET" and len(args) >= 2:
                    key, value = args[0], args[1]
                    expiry = None
                    
                    # Check for PX argument
                    if len(args) >= 4 and args[2].upper() == "PX":
                        try:
                            px_value = int(args[3])
                            expiry = get_current_time_ms() + px_value
                        except ValueError:
                            writer.write(b"-ERR value is not an integer or out of range\r\n")
                            continue
                    
                    data_store[key] = (value, expiry)
                    writer.write(b"+OK\r\n")
                elif command == "GET":
                    if len(args) >= 1:
                        key = args[0]
                        if key in data_store and not is_key_expired(key):
                            value, _ = data_store[key]
                            resp = f"${len(value)}\r\n{value}\r\n"
                            writer.write(resp.encode())
                        else:
                            # Remove expired key if it exists
                            if key in data_store and is_key_expired(key):
                                del data_store[key]
                            writer.write(b"$-1\r\n")  # Redis nil response
                    else:
                        writer.write(b"-ERR wrong number of arguments for 'get' command\r\n")
                else:
                    # Default response for unknown commands
                    writer.write(b"-ERR unknown command\r\n")
                
                await writer.drain()
                
        except Exception as e:
            print(f"Error handling client: {e}")
            writer.write(b"-ERR internal error\r\n")
            await writer.drain()
            break
    
    print("Client disconnected")
    writer.close()

async def connect_to_master(host: str, port: int, replica_port: int):
    """Establish connection to master server"""
    try:
        reader, writer = await asyncio.open_connection(host, port)
        print(f"Connected to master at {host}:{port}")
        
        # Send PING to check connection
        ping_command = b"*1\r\n$4\r\nPING\r\n"
        print(f"Sending to master: {ping_command!r}")
        writer.write(ping_command)
        await writer.drain()
        
        # Read PONG response
        response = await reader.read(1024)
        print(f"Received from master: {response!r}")
        if not response:
            print("Failed to receive PING response from master")
            writer.close()
            return None, None
            
        # Send first REPLCONF command (listening-port)
        port_cmd = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(replica_port))}\r\n{replica_port}\r\n"
        print(f"Sending REPLCONF listening-port: {port_cmd!r}")
        writer.write(port_cmd.encode())
        await writer.drain()
        
        # Read OK response
        response = await reader.read(1024)
        print(f"Received from master: {response!r}")
        if not response or b"+OK" not in response:
            print("Failed to receive OK response for REPLCONF listening-port")
            writer.close()
            return None, None
            
        # Send second REPLCONF command (capa psync2)
        capa_cmd = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        print(f"Sending REPLCONF capa: {capa_cmd!r}")
        writer.write(capa_cmd)
        await writer.drain()
        
        # Read OK response
        response = await reader.read(1024)
        print(f"Received from master: {response!r}")
        if not response or b"+OK" not in response:
            print("Failed to receive OK response for REPLCONF capa")
            writer.close()
            return None, None
            
        # Send PSYNC command
        psync_cmd = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        print(f"Sending PSYNC: {psync_cmd!r}")
        writer.write(psync_cmd)
        await writer.drain()
        
        # Read FULLRESYNC response (we'll ignore the details for now)
        response = await reader.read(1024)
        print(f"Received from master: {response!r}")
        if not response or not response.startswith(b"+FULLRESYNC"):
            print("Failed to receive FULLRESYNC response")
            writer.close()
            return None, None
            
        return reader, writer
    except Exception as e:
        print(f"Failed to connect to master: {e}")
        return None, None

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--port', type=int, default=6379, help='Port to listen on')
    parser.add_argument('--replicaof', type=str, help='Master host and port (e.g. "localhost 6379")')
    args = parser.parse_args()
    
    # Configure server role based on --replicaof flag
    if args.replicaof:
        try:
            master_host, master_port = args.replicaof.split()
            server_config.update({
                "role": "slave",
                "master_host": master_host,
                "master_port": int(master_port)
            })
            print(f"Running as replica of {master_host}:{master_port}")
            
            # Connect to master before starting server
            master_reader, master_writer = await connect_to_master(
                master_host, 
                int(master_port),
                args.port  # Pass the replica's port
            )
            if not master_reader or not master_writer:
                print("Failed to establish connection with master")
                return
            
        except ValueError:
            print("Error: --replicaof argument must be in format 'host port'")
            return
    
    # Start server with specified port
    server = await asyncio.start_server(handle_client, "localhost", args.port)
    print(f"Server listening on port {args.port}...")
    
    if args.replicaof:
        # Start both the server and master connection handling
        async with server:
            await asyncio.gather(
                server.serve_forever(),
                handle_master_connection(master_reader, master_writer)
            )
    else:
        # Just run the server normally
        async with server:
            await server.serve_forever()

async def handle_master_connection(reader, writer):
    """Handle ongoing communication with master"""
    try:
        while True:
            # Keep connection alive and handle master commands
            data = await reader.read(1024)
            if not data:
                print("Master connection closed")
                break
            
            # Handle master commands here (will be implemented in future stages)
            print(f"Received from master: {data}")
            
    except Exception as e:
        print(f"Error in master connection: {e}")
    finally:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
