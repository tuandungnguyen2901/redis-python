import socket
import asyncio

# Add a dictionary to store key-value pairs
data_store = {}

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
            # Add SET command handler
            elif command == "SET" and len(args) >= 2:
                key, value = args[0], args[1]
                data_store[key] = value
                writer.write(b"+OK\r\n")
            # Add GET command handler
            elif command == "GET":
                if len(args) >= 1:
                    key = args[0]
                    value = data_store.get(key)
                    if value is not None:
                        resp = f"${len(value)}\r\n{value}\r\n"
                        writer.write(resp.encode())
                    else:
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
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    print("Server listening on port 6379...")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
