from typing import Set, Optional, Tuple
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
                            
                            # Process the command
                            if command:
                                print(f"Processing master command: {command} {args}")
                                
                                # Execute the command on replica
                                if command == "REPLCONF" and len(args) >= 2 and args[0].upper() == "GETACK":
                                    print("Received REPLCONF GETACK, sending ACK")
                                    ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                                    writer.write(ack_response)
                                    await writer.drain()
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
                if remaining_data and remaining_data.startswith(b"*3\r\n$8\r\nREPLCONF"):
                    print(f"Found command after RDB: {remaining_data}")
                    if b"GETACK" in remaining_data:
                        print("Handling REPLCONF GETACK command")
                        ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                        writer.write(ack_response)
                        await writer.drain()
                else:
                    # Check for additional commands that might arrive separately
                    try:
                        cmd_data = await asyncio.wait_for(reader.read(1024), 0.1)
                        if cmd_data and cmd_data.startswith(b"*3\r\n$8\r\nREPLCONF"):
                            print(f"Received command after RDB: {cmd_data}")
                            if b"GETACK" in cmd_data:
                                print("Handling REPLCONF GETACK command")
                                ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", "0"])
                                writer.write(ack_response)
                                await writer.drain()
                    except asyncio.TimeoutError:
                        # No immediate command, that's fine
                        pass
                
                # After receiving the RDB file, wait just a moment to ensure we're ready for commands
                await asyncio.sleep(0.1)
                
                # Clear any data that might be in the reader buffer
                try:
                    reader._buffer.clear()  # Reset the reader buffer
                except:
                    # If this fails, it's not critical
                    pass
                
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