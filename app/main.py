import socket
import asyncio

async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    print("Connected", addr)
    while True:
        data = await reader.read(1024)
        if not data:
            break
        
        message = data.decode()
        print("Data:", message)
        
        # Parse the command
        parts = message.strip().split()
        command = parts[0].upper() if parts else ""
        
        if command == "PING":
            writer.write(b"+PONG\r\n")
        elif command == "ECHO" and len(parts) > 1:
            echo_arg = parts[1]
            # Format as RESP bulk string
            resp = f"${len(echo_arg)}\r\n{echo_arg}\r\n"
            writer.write(resp.encode())
        else:
            # Default response for unknown commands
            writer.write(b"+PONG\r\n")
            
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
