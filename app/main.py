import socket
import asyncio
import time
import argparse
from typing import Dict, Tuple, Optional

# Store both value and expiry timestamp (in ms since epoch)
data_store: Dict[str, Tuple[str, Optional[int]]] = {}

def get_current_time_ms():
    return int(time.time() * 1000)

def is_key_expired(key):
    if key not in data_store:
        return True
    value, expiry = data_store[key]
    if expiry is None:
        return False
    return get_current_time_ms() >= expiry

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print("Connected", addr)
    while True:
        data = await reader.read(1024)
        if not data:
            break
        
        message = data.decode()
        print("Data:", message)
        
        # Parse RESP protocol input
        lines = message.split('\r\n')
        print("Parsed lines:", lines)
        
        # Handle RESP array format
        command = None
        args = []
        
        try:
            # Check if it's an array command
            if lines[0].startswith('*'):
                array_length = int(lines[0][1:])
                item_index = 1
                
                for i in range(array_length):
                    if item_index < len(lines) and lines[item_index].startswith('$'):
                        bulk_length = int(lines[item_index][1:])
                        item_index += 1
                        
                        if item_index < len(lines):
                            if command is None:
                                command = lines[item_index].upper()
                                print(f"Command detected: {command}")
                            else:
                                args.append(lines[item_index])
                                print(f"Argument detected: {lines[item_index]}")
                            
                            item_index += 1
            else:
                # Simple command parsing (fallback)
                parts = message.strip().split()
                command = parts[0].upper() if parts else ""
                args = parts[1:] if len(parts) > 1 else []
                
            print(f"Processed command: {command}, args: {args}")
                
            # Execute the command
            if command == "PING":
                writer.write(b"+PONG\r\n")
            elif command == "ECHO" and args:
                echo_arg = args[0]
                # Format as RESP bulk string
                resp = f"${len(echo_arg)}\r\n{echo_arg}\r\n"
                writer.write(resp.encode())
                print(f"Sending ECHO response: {resp}")
            # Updated SET command handler with PX support
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
            # Updated GET command handler with expiry check
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
                
        except Exception as e:
            print(f"Error parsing command: {e}")
            writer.write(b"-ERR parsing error\r\n")
            
        await writer.drain()
    
    print("Client disconnected")
    writer.close()

async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Redis server implementation')
    parser.add_argument('--port', type=int, default=6379, help='Port to listen on')
    args = parser.parse_args()
    
    # Start server with specified port
    server = await asyncio.start_server(handle_client, "localhost", args.port)
    print(f"Server listening on port {args.port}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
