from typing import Set, Optional, Tuple, List
from asyncio import StreamWriter, StreamReader
import asyncio
from resp import RESPProtocol
import gc

class ReplicationManager:
    """Handle Redis replication logic"""
    
    EMPTY_RDB_HEX = "524544495330303131FE00FF77DE0394AC9D23EA"
    
    def __init__(self):
        self.role: str = "master"
        self.replicas: Set[StreamWriter] = set()
        self.master_host: Optional[str] = None
        self.master_port: Optional[int] = None
        self.replica_port: Optional[int] = None
        self.master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset: int = 0
        # Add a counter to track bytes processed from master
        self.processed_bytes: int = 0
    
    def get_empty_rdb(self) -> bytes:
        """Return empty RDB file contents"""
        return bytes.fromhex(self.EMPTY_RDB_HEX)
    
    async def propagate_to_replicas(self, command: str, *args: str) -> None:
        """Propagate command to all connected replicas"""
        if not self.replicas:
            return
            
        try:
            cmd_bytes = RESPProtocol.encode_array([command, *args])
            for writer in self.replicas:
                if not writer.is_closing():
                    writer.write(cmd_bytes)
                    await writer.drain()
        except Exception as e:
            print(f"Error propagating command: {e}")
    
    async def handle_master_connection(self, reader: StreamReader, writer: StreamWriter) -> None:
        """Handle ongoing communication with master"""
        buffer = b""
        redis_instance = None
        
        # This is a bit of a hack - we need to get a reference to the Redis instance
        # that owns this ReplicationManager. We could also pass it as a parameter.
        from redis_server import Redis
        for obj in gc.get_objects():
            if isinstance(obj, Redis) and obj.replication is self:
                redis_instance = obj
                break
        
        if not redis_instance:
            print("Error: Could not find Redis instance")
            return
        
        try:
            print("Starting master connection handler")
            while True:
                try:
                    data = await reader.read(1024)
                    if not data:
                        print("Master connection closed")
                        break
                    
                    # Append to buffer and process any complete commands
                    buffer += data
                    print(f"Received data from master ({len(data)} bytes): {data[:50]!r}...")
                    
                    # Process commands from buffer
                    while buffer:
                        # Check for complete command
                        if not buffer.startswith(b"*"):
                            # Invalid format, clear buffer
                            print(f"Invalid data received from master: {buffer[:20]!r}...")
                            buffer = b""
                            break
                        
                        # Parse RESP array command
                        try:
                            # Find array length from first line
                            first_line_end = buffer.find(b"\r\n")
                            if first_line_end == -1:
                                print("Incomplete command (no first line end)")
                                break  # Incomplete command
                            
                            array_length = int(buffer[1:first_line_end])
                            print(f"Parsing array command with length {array_length}")
                            
                            # Count expected \r\n
                            expected_crlf = 1 + (array_length * 2)  # 1 for array marker, 2 per item (length+value)
                            
                            # Find all \r\n
                            crlf_count = 0
                            pos = 0
                            while True:
                                pos = buffer.find(b"\r\n", pos)
                                if pos == -1:
                                    break
                                crlf_count += 1
                                pos += 2
                            
                            if crlf_count < expected_crlf:
                                print(f"Incomplete command (expected {expected_crlf} CRLF, got {crlf_count})")
                                break  # Incomplete command
                            
                            # Parse command and args
                            command = None
                            args = []
                            
                            lines = buffer.split(b"\r\n")
                            index = 1  # Skip the array length line
                            
                            for i in range(array_length):
                                if index >= len(lines) or not lines[index].startswith(b"$"):
                                    break
                                    
                                # Skip the length marker
                                index += 1
                                
                                if index >= len(lines):
                                    break
                                    
                                # Get the value
                                value = lines[index].decode("utf-8")
                                if i == 0:
                                    command = value.upper()
                                else:
                                    args.append(value)
                                    
                                index += 1
                            
                            # Calculate command length in bytes to remove from buffer
                            cmd_end = 0
                            for i in range(expected_crlf):
                                cmd_end = buffer.find(b"\r\n", cmd_end) + 2
                            
                            # Get the actual command bytes for offset tracking
                            command_bytes = buffer[:cmd_end]
                            command_length = len(command_bytes)
                            
                            # Process the command
                            if command:
                                print(f"Processing master command: {command} {args}")
                                
                                # Execute the command on replica
                                if command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                    # For GETACK, respond with current offset but don't add command to offset yet
                                    print(f"Received REPLCONF GETACK, sending ACK with offset {self.processed_bytes}")
                                    ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", str(self.processed_bytes)])
                                    writer.write(ack_response)
                                    await writer.drain()
                                    
                                    # After responding, add this command's bytes to the offset
                                    self.processed_bytes += command_length
                                    print(f"Updated processed bytes to {self.processed_bytes} after GETACK")
                                elif command == "SET" and len(args) >= 2:
                                    key, value = args[0], args[1]
                                    expiry = None
                                    
                                    # Handle PX argument
                                    if len(args) >= 4 and args[2].upper() == "PX":
                                        try:
                                            px_value = int(args[3])
                                            expiry = redis_instance.get_current_time_ms() + px_value
                                        except ValueError:
                                            print(f"Invalid PX value: {args[3]}")
                                    
                                    # Store in redis instance (directly to prevent expiration issues)
                                    print(f"Setting key from master: {key} = {value}")
                                    redis_instance.data_store[key] = (value, expiry)
                                    print(f"Data store after SET: {redis_instance.data_store}")
                                    
                                    # Add this command's bytes to the offset
                                    self.processed_bytes += command_length
                                    print(f"Updated processed bytes to {self.processed_bytes} after SET")
                                else:
                                    # For any other command, just update the offset
                                    self.processed_bytes += command_length
                                    print(f"Updated processed bytes to {self.processed_bytes} after {command}")
                                
                                # Add other command handlers as needed
                                
                            # Remove the processed command from buffer
                            buffer = buffer[cmd_end:]
                            print(f"Remaining buffer after command: {len(buffer)} bytes")
                            
                        except (ValueError, IndexError) as e:
                            print(f"Error parsing command from master: {e}")
                            if b"\r\n" in buffer:
                                buffer = buffer[buffer.find(b"\r\n") + 2:]
                            else:
                                buffer = b""
                except Exception as e:
                    print(f"Error reading from master: {e}")
                    # Don't break here, just continue trying to read
                    await asyncio.sleep(0.1)
                        
        except Exception as e:
            print(f"Error in master connection: {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            print("Master connection handler finished")
    
    async def connect_to_master(self) -> Tuple[Optional[StreamReader], Optional[StreamWriter]]:
        """Establish connection to master and perform handshake"""
        try:
            if not self.master_host or not self.master_port:
                return None, None
                
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            print(f"Connected to master at {self.master_host}:{self.master_port}")
            
            # Send PING
            writer.write(b"*1\r\n$4\r\nPING\r\n")
            await writer.drain()
            
            # Read PONG
            response = await reader.read(1024)
            if not response or not response.startswith(b"+PONG"):
                print(f"Unexpected PING response: {response!r}")
                writer.close()
                return None, None
            print(f"Received PING response: {response!r}")
            
            # Send REPLCONF listening-port with replica's own port
            port_str = str(self.replica_port)
            port_cmd = RESPProtocol.encode_array(["REPLCONF", "listening-port", port_str])
            writer.write(port_cmd)
            await writer.drain()
            
            # Read OK for listening-port
            response = await reader.read(1024)
            if not response or not response.startswith(b"+OK"):
                print(f"Unexpected REPLCONF response: {response!r}")
                writer.close()
                return None, None
            print(f"Received REPLCONF response: {response!r}")
            
            # Send REPLCONF capa psync2
            capa_cmd = RESPProtocol.encode_array(["REPLCONF", "capa", "psync2"])
            writer.write(capa_cmd)
            await writer.drain()
            
            # Read OK for capa
            response = await reader.read(1024)
            if not response or not response.startswith(b"+OK"):
                print(f"Unexpected REPLCONF capa response: {response!r}")
                writer.close()
                return None, None
            print(f"Received REPLCONF capa response: {response!r}")
            
            # Send PSYNC
            psync_cmd = RESPProtocol.encode_array(["PSYNC", "?", "-1"])
            writer.write(psync_cmd)
            await writer.drain()
            
            # Read FULLRESYNC response
            response = await reader.read(8192)  # Larger buffer to get more data
            
            # Check for FULLRESYNC
            if not response or b"+FULLRESYNC" not in response:
                print(f"Unexpected PSYNC response: {response!r}")
                writer.close()
                return None, None
            
            print(f"Received PSYNC response: {response!r}")
            
            # Parse RDB from the response
            try:
                # Find the RDB header
                rdb_marker_pos = response.find(b"$")
                if rdb_marker_pos == -1:
                    # Wait for separate RDB message if not included in FULLRESYNC response
                    rdb_response = await reader.read(1024)
                    if not rdb_response or not rdb_response.startswith(b"$"):
                        print(f"Unexpected RDB header: {rdb_response!r}")
                        writer.close()
                        return None, None
                    rdb_data = rdb_response
                else:
                    rdb_data = response[rdb_marker_pos:]
                
                # Find length marker end
                length_end = rdb_data.find(b"\r\n")
                if length_end == -1:
                    raise ValueError("No length terminator found")
                
                # Parse RDB length
                rdb_length = int(rdb_data[1:length_end])
                print(f"Expected RDB length: {rdb_length}")
                
                # Extract RDB content so far
                rdb_content_start = length_end + 2  # Skip \r\n after length
                
                # Calculate where RDB content should end
                rdb_content_end = rdb_content_start + rdb_length
                
                # Check if REPLCONF command starts immediately after RDB
                remaining_data = b""
                if len(rdb_data) > rdb_content_end and rdb_data[rdb_content_end:].startswith(b"*"):
                    remaining_data = rdb_data[rdb_content_end:]
                    rdb_content = rdb_data[rdb_content_start:rdb_content_end]
                else:
                    rdb_content = rdb_data[rdb_content_start:]
                    
                    # Continue reading RDB content if needed
                    remaining_bytes = rdb_length - len(rdb_content)
                    while remaining_bytes > 0:
                        chunk = await reader.read(min(4096, remaining_bytes))
                        if not chunk:
                            raise ValueError("Connection closed while reading RDB")
                            
                        if len(chunk) > remaining_bytes:
                            # We got more than needed, which might include commands
                            rdb_content += chunk[:remaining_bytes]
                            remaining_data = chunk[remaining_bytes:]
                            remaining_bytes = 0
                        else:
                            rdb_content += chunk
                            remaining_bytes -= len(chunk)
                
                print(f"Received complete RDB file of length {len(rdb_content)}")
                
                # Check for and handle REPLCONF GETACK in the remaining data
                if remaining_data and remaining_data.startswith(b"*"):
                    print(f"Found command after RDB: {remaining_data}")
                    
                    # Process the initial command(s) in the remaining data
                    cmd_end = self.find_command_end(remaining_data)
                    if cmd_end > 0:
                        initial_cmd = remaining_data[:cmd_end]
                        
                        # Check if it's a REPLCONF GETACK command
                        if b"REPLCONF" in initial_cmd and b"GETACK" in initial_cmd:
                            print("Handling initial REPLCONF GETACK command")
                            
                            # For the very first GETACK after RDB, respond with 0
                            ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                            writer.write(ack_response)
                            await writer.drain()
                            
                            # Add the command bytes to the processed_bytes counter
                            self.processed_bytes = len(initial_cmd)
                            print(f"Initialized processed bytes to {self.processed_bytes}")
                            
                            # Remove the processed command from remaining data
                            remaining_data = remaining_data[cmd_end:]
                
                # Process remaining data from RDB response
                if remaining_data:
                    print(f"Found command data after RDB: {remaining_data}")
                    
                    # Process all complete commands in the remaining data
                    while remaining_data.startswith(b"*"):
                        cmd_end = self.find_command_end(remaining_data)
                        if cmd_end == 0:
                            break  # Incomplete command
                        
                        command_data = remaining_data[:cmd_end]
                        
                        # Parse the command
                        try:
                            command, args = self.parse_command_bytes(command_data)
                            print(f"Parsed command after RDB: {command} {args}")
                            
                            # Handle the command
                            if command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                # For the first GETACK, respond with 0
                                print("Handling REPLCONF GETACK command after RDB")
                                ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                                writer.write(ack_response)
                                await writer.drain()
                            
                            # Track processed bytes for all commands
                            self.processed_bytes += len(command_data)
                            print(f"Updated processed bytes to {self.processed_bytes} after {command}")
                            
                        except Exception as e:
                            print(f"Error parsing command: {e}")
                        
                        # Move to next command
                        remaining_data = remaining_data[cmd_end:]
                
                # Try to read more commands if needed
                try:
                    cmd_data = await asyncio.wait_for(reader.read(1024), 0.2)
                    if cmd_data:
                        print(f"Received additional command data: {cmd_data}")
                        
                        # Process the data the same way as the remaining data
                        pos = 0
                        while pos < len(cmd_data):
                            if not cmd_data[pos:].startswith(b"*"):
                                break
                            
                            # Find end of this command
                            cmd_end = self.find_command_end(cmd_data[pos:])
                            if cmd_end == 0:
                                break  # Incomplete command
                            
                            command_bytes = cmd_data[pos:pos+cmd_end]
                            
                            # Parse and handle command
                            try:
                                command, args = self.parse_command_bytes(command_bytes)
                                print(f"Parsed additional command: {command} {args}")
                                
                                if command == "PING":
                                    print("Received PING from master")
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after PING")
                                elif command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                    print(f"Received REPLCONF GETACK, responding with offset {self.processed_bytes}")
                                    ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", str(self.processed_bytes)])
                                    writer.write(ack_response)
                                    await writer.drain()
                                    
                                    # After responding, add this command's bytes to the processed bytes
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after GETACK")
                                else:
                                    # Process any other command
                                    self.processed_bytes += len(command_bytes)
                                    print(f"Updated processed bytes to {self.processed_bytes} after {command}")
                                
                            except Exception as e:
                                print(f"Error parsing additional command: {e}")
                            
                            # Move to next command
                            pos += cmd_end
                except asyncio.TimeoutError:
                    print("No additional commands received")
                
                return reader, writer
                
            except (ValueError, IndexError) as e:
                print(f"Error processing RDB: {e}")
                writer.close()
                return None, None
            
        except Exception as e:
            print(f"Failed to connect to master: {e}")
            return None, None

    async def handle_replconf(self, args: list, writer: StreamWriter) -> None:
        """Handle REPLCONF command from replica"""
        print(f"Received REPLCONF with args: {args}")
        # Add writer to replicas set if this is a replication connection
        if args and args[0] == "listening-port":
            self.replicas.add(writer)
        writer.write(RESPProtocol.encode_simple_string("OK"))
        await writer.drain()

    async def handle_psync(self, args: list, writer: StreamWriter) -> None:
        """Handle PSYNC command from replica"""
        print(f"Received PSYNC with args: {args}")
        response = f"+FULLRESYNC {self.master_replid} {self.master_repl_offset}\r\n"
        writer.write(response.encode())
        await writer.drain()
        
        # Send empty RDB file
        rdb_contents = self.get_empty_rdb()
        rdb_length = len(rdb_contents)
        writer.write(f"${rdb_length}\r\n".encode())
        writer.write(rdb_contents)
        await writer.drain()
        
        # Add this connection to replicas set if not already added
        self.replicas.add(writer)

    def cleanup_replicas(self) -> None:
        """Remove closed connections from replicas set"""
        self.replicas = {w for w in self.replicas if not w.is_closing()} 

    def find_command_end(self, buffer: bytes) -> int:
        """Find the end position of a complete RESP command in the buffer"""
        if not buffer.startswith(b"*"):
            # Not a RESP array
            return 0
        
        try:
            # Find array length
            first_line_end = buffer.find(b"\r\n")
            if first_line_end == -1:
                return 0  # Incomplete command
            
            array_length = int(buffer[1:first_line_end])
            
            # Expected number of \r\n pairs
            expected_crlf = 1 + (array_length * 2)  # 1 for array marker, 2 per item
            
            # Find all \r\n occurrences
            pos = 0
            for _ in range(expected_crlf):
                pos = buffer.find(b"\r\n", pos)
                if pos == -1:
                    return 0  # Incomplete command
                pos += 2
            
            return pos  # End position of the command
            
        except (ValueError, IndexError):
            # Invalid format
            return 0 

    def parse_command_bytes(self, data: bytes) -> Tuple[str, List[str]]:
        """Parse a RESP command from bytes"""
        command = None
        args = []
        
        try:
            lines = data.split(b"\r\n")
            array_length = int(lines[0][1:])  # Skip the * in *3
            
            idx = 1
            for i in range(array_length):
                if idx < len(lines) and lines[idx].startswith(b"$"):
                    # Skip the length line
                    idx += 1
                    
                    if idx < len(lines):
                        value = lines[idx].decode("utf-8")
                        if i == 0:
                            command = value.upper()
                        else:
                            args.append(value)
                    idx += 1
        except Exception as e:
            print(f"Error parsing command bytes: {e}")
        
        return command, args 