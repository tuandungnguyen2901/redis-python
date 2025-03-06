import socket
import asyncio
import time
import argparse
from typing import Dict, Tuple, Optional, Set
import base64
from asyncio import StreamReader, StreamWriter

from resp import RESPProtocol
from replication import ReplicationManager

class Redis:
    def __init__(self, port: int):
        self.port = port
        self.data_store: Dict[str, Tuple[str, Optional[int]]] = {}
        self.replication = ReplicationManager()
        
    def get_current_time_ms(self) -> int:
        return int(time.time() * 1000)
        
    def is_key_expired(self, key: str) -> bool:
        if key not in self.data_store:
            return True
        value, expiry = self.data_store[key]
        if expiry is None:
            return False
        return self.get_current_time_ms() >= expiry
        
    def format_info_response(self, section: Optional[str] = None) -> str:
        if section == "replication":
            info_lines = [
                f"role:{self.replication.role}",
                f"master_replid:{self.replication.master_replid}",
                f"master_repl_offset:{self.replication.master_repl_offset}"
            ]
            info_str = "\n".join(info_lines)
            return f"${len(info_str)}\r\n{info_str}\r\n"
        return "$-1\r\n"
        
    async def handle_client(self, reader: StreamReader, writer: StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        print(f"Connected {addr}")
        buffer = b""  # Change to bytes buffer
        
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                    
                # Work with bytes directly instead of decoding
                buffer += data
                
                # Process complete commands
                while b"\r\n" in buffer:
                    if buffer.startswith(b"PING"):
                        # Simple text protocol (non-RESP)
                        writer.write(b"+PONG\r\n")
                        await writer.drain()
                        buffer = buffer[4:]  # Remove "PING"
                        if buffer.startswith(b"\r\n"):
                            buffer = buffer[2:]  # Remove \r\n
                        continue
                    
                    # For RESP array commands
                    if buffer.startswith(b"*"):
                        # Find the first line to get array length
                        first_line_end = buffer.find(b"\r\n")
                        if first_line_end == -1:
                            break  # Incomplete command
                            
                        try:
                            # Get array length
                            array_length = int(buffer[1:first_line_end])
                            
                            # Check if we have the complete command
                            command_end = first_line_end + 2  # Skip first \r\n
                            count = 0
                            
                            # Find the end of the command by counting \r\n pairs
                            for _ in range(array_length * 2):  # Each item has $len\r\nvalue\r\n
                                next_end = buffer.find(b"\r\n", command_end)
                                if next_end == -1:
                                    break
                                command_end = next_end + 2
                                count += 1
                                
                            if count < array_length * 2:
                                break  # Incomplete command
                                
                            # Extract and process the command
                            command_data = buffer[:command_end]
                            buffer = buffer[command_end:]
                            
                            # Parse the command
                            lines = command_data.split(b"\r\n")
                            command = None
                            args = []
                            
                            i = 1
                            while i < len(lines) and i < array_length * 2:
                                if lines[i].startswith(b"$"):
                                    i += 1
                                    if i < len(lines):
                                        if command is None:
                                            command = lines[i].decode().upper()
                                        else:
                                            args.append(lines[i].decode())
                                    i += 1
                            
                            # Handle the parsed command
                            await self._execute_command(command, args, writer)
                        except (ValueError, IndexError) as e:
                            print(f"Error parsing command: {e}")
                            writer.write(b"-ERR protocol error\r\n")
                            await writer.drain()
                            if b"\r\n" in buffer:
                                buffer = buffer[buffer.find(b"\r\n") + 2:]
                            else:
                                buffer = b""
                    else:
                        # Simple command format or invalid
                        line_end = buffer.find(b"\r\n")
                        if line_end == -1:
                            break  # Incomplete command
                            
                        line = buffer[:line_end].decode().strip()
                        buffer = buffer[line_end + 2:]  # Remove the processed line
                        
                        parts = line.split()
                        if parts:
                            command = parts[0].upper()
                            args = parts[1:] if len(parts) > 1 else []
                            await self._execute_command(command, args, writer)
                    
        except Exception as e:
            print(f"Error handling client: {e}")
            writer.write(b"-ERR internal error\r\n")
            await writer.drain()
            
        print("Client disconnected")
        writer.close()
        self.replication.cleanup_replicas()
        
    async def _execute_command(self, command: str, args: list, writer: StreamWriter) -> None:
        """Execute Redis command and send response"""
        try:
            print(f"Executing command: {command}, args: {args}")
            
            if command == "PING":
                writer.write(b"+PONG\r\n")
            elif command == "ECHO" and args:
                writer.write(RESPProtocol.encode_bulk_string(args[0]))
            elif command == "REPLCONF":
                await self.replication.handle_replconf(args, writer)
            elif command == "PSYNC":
                await self.replication.handle_psync(args, writer)
            elif command == "INFO":
                section = args[0].lower() if args else None
                writer.write(self.format_info_response(section).encode())
            elif command == "SET" and len(args) >= 2:
                await self._handle_set(args, writer)
            elif command == "GET":
                await self._handle_get(args, writer)
            elif command == "WAIT":
                # Process WAIT command - wait for replica acknowledgments
                num_replicas = 0
                timeout_ms = 0
                
                # Parse arguments
                if len(args) >= 1:
                    try:
                        num_replicas = int(args[0])
                    except ValueError:
                        writer.write(RESPProtocol.encode_error("value is not an integer"))
                        await writer.drain()
                        return
                        
                if len(args) >= 2:
                    try:
                        timeout_ms = int(args[1])
                    except ValueError:
                        writer.write(RESPProtocol.encode_error("timeout is not an integer"))
                        await writer.drain()
                        return
                
                # First clean up any closed connections
                self.replication.cleanup_replicas()
                replica_count = len(self.replication.replicas)
                
                print(f"WAIT command: waiting for {num_replicas} replicas (have {replica_count}) with timeout {timeout_ms}ms")
                
                # If no replicas connected or requested count is 0, return immediately
                if replica_count == 0 or num_replicas == 0:
                    writer.write(RESPProtocol.encode_integer(0))
                    await writer.drain()
                    return
                
                # Check if we need to wait (if we have fewer replicas than requested)
                if replica_count < num_replicas:
                    # Just wait for the timeout, then return what we have
                    start_time = self.get_current_time_ms()
                    end_time = start_time + timeout_ms
                    
                    # Send REPLCONF GETACK to all replicas
                    for replica_writer in self.replication.replicas:
                        if not replica_writer.is_closing():
                            try:
                                getack_cmd = RESPProtocol.encode_array(["REPLCONF", "GETACK", "*"])
                                replica_writer.write(getack_cmd)
                                await replica_writer.drain()
                            except Exception as e:
                                print(f"Error sending GETACK to replica: {e}")
                    
                    # Wait for timeout
                    while self.get_current_time_ms() < end_time:
                        await asyncio.sleep(0.01)  # Small sleep to avoid hogging CPU
                
                # Return the actual count of acked replicas (for now, just all connected replicas)
                # In a real implementation, we'd track which replicas actually acked
                replica_count = len(self.replication.replicas)
                writer.write(RESPProtocol.encode_integer(replica_count))
            else:
                writer.write(RESPProtocol.encode_error("unknown command"))
            
            await writer.drain()
            
        except Exception as e:
            print(f"Error executing command: {e}")
            writer.write(RESPProtocol.encode_error("execution error"))
            await writer.drain()
        
    async def _handle_set(self, args: list, writer: StreamWriter) -> None:
        key, value = args[0], args[1]
        expiry = None
        
        if len(args) >= 4 and args[2].upper() == "PX":
            try:
                px_value = int(args[3])
                expiry = self.get_current_time_ms() + px_value
            except ValueError:
                writer.write(RESPProtocol.encode_error("value is not an integer or out of range"))
                return
                
        self.data_store[key] = (value, expiry)
        writer.write(RESPProtocol.encode_simple_string("OK"))
        
        await self.replication.propagate_to_replicas("SET", key, value, *args[2:])
        
    async def _handle_get(self, args: list, writer: StreamWriter) -> None:
        """Handle GET command"""
        if len(args) == 1:
            key = args[0]
            
            # Check if key exists
            if key in self.data_store:
                # Check for expiration
                if self.is_key_expired(key):
                    del self.data_store[key]  # Delete expired key
                    writer.write(RESPProtocol.encode_bulk_string(None))  # Return nil for expired key
                else:
                    # Return the value (not expired)
                    value, _ = self.data_store[key]
                    print(f"GET {key} returning value: {value}")
                    writer.write(RESPProtocol.encode_bulk_string(value))
            else:
                # Key doesn't exist
                writer.write(RESPProtocol.encode_bulk_string(None))
        else:
            # Wrong number of arguments
            writer.write(RESPProtocol.encode_error("wrong number of arguments for 'get' command"))
            
    async def start(self) -> None:
        if self.replication.role == "slave":
            master_reader, master_writer = await self.replication.connect_to_master()
            if not master_reader or not master_writer:
                print("Failed to establish connection with master")
                return
                
        server = await asyncio.start_server(self.handle_client, "localhost", self.port)
        print(f"Server listening on port {self.port}...")
        
        if self.replication.role == "slave":
            async with server:
                await asyncio.gather(
                    server.serve_forever(),
                    self.replication.handle_master_connection(master_reader, master_writer)
                )
        else:
            async with server:
                await server.serve_forever()
