# Uncomment this to pass the first stage
import socket
import asyncio

async def handle_client(reader, writer):
    pong = b"+PONG\r\n"
    addr = writer.get_extra_info("peername")
    print("Connected", addr)
    while True:
        data = await reader.read(1024)
        if not data:
            break
        print("Data:", data.decode())
        writer.write(pong)
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
