from typing import Set, Optional, Tuple, List, Dict
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
        self.replica_ack_offsets: Dict[StreamWriter, int] = {}  # Track ACK offsets per replica
        self.master_host: Optional[str] = None
        self.master_port: Optional[int] = None
        self.replica_port: Optional[int] = None
        self.master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.master_repl_offset: int = 0
        # Add a counter to track bytes processed from master
        self.processed_bytes: int = 0
        self.has_pending_writes: bool = False  # Track if we have any pending write operations
    
    def get_empty_rdb(self) -> bytes:
        """Return empty RDB file contents"""
        return bytes.fromhex(self.EMPTY_RDB_HEX)
    
    async def propagate_to_replicas(self, command: str, *args: str) -> None:
        """Propagate command to all connected replicas"""
        if not self.replicas:
            return
            
        try:
            # For INCR, we need to make sure args are properly converted to strings
            cmd_args = [command]
            for arg in args:
                cmd_args.append(str(arg))
            
            cmd_bytes = RESPProtocol.encode_array(cmd_args)
            
            print(f"Propagating to replicas: {command} {args}")
            
            for writer in self.replicas:
                if not writer.is_closing():
                    writer.write(cmd_bytes)
                    await writer.drain()
                    
            # Increment replication offset based on command size
            # Each command sent increments the offset
            self.master_repl_offset += len(cmd_bytes)
            
            # Mark that we have pending writes that need to be acknowledged
            self.has_pending_writes = True
            
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
                                    
                                    # Save to RDB after key change from master
                                    await redis_instance._save_rdb()
                                    
                                    # Add this command's bytes to the offset
                                    self.processed_bytes += command_length
                                    print(f"Updated processed bytes to {self.processed_bytes} after SET")
                                elif command == "KEYS" and len(args) >= 1:
                                    # Handle KEYS command (useful for debugging)
                                    pattern = args[0]
                                    matched_keys = redis_instance._get_matching_keys(pattern)
                                    print(f"Keys matching pattern {pattern}: {matched_keys}")
                                    
                                    # Update processed bytes
                                    self.processed_bytes += command_length
                                elif command == "INCR" and len(args) >= 1:
                                    # Handle INCR command from master
                                    key = args[0]
                                    
                                    # Check if key exists and is not expired
                                    if key in redis_instance.data_store and not redis_instance.is_key_expired(key):
                                        value, expiry = redis_instance.data_store[key]
                                        
                                        try:
                                            # Try to convert value to integer and increment
                                            int_value = int(value)
                                            int_value += 1
                                            
                                            # Store the new value (preserving expiry)
                                            redis_instance.data_store[key] = (str(int_value), expiry)
                                            print(f"Incremented key from master: {key} = {int_value}")
                                            
                                            # Save to RDB after key change from master
                                            await redis_instance._save_rdb()
                                        except ValueError:
                                            print(f"Error incrementing key {key}: value is not an integer")
                                    else:
                                        # Key doesn't exist - create it with value "1"
                                        redis_instance.data_store[key] = ("1", None)  # No expiry
                                        print(f"Created new key from master INCR: {key} = 1")
                                        
                                        # Save to RDB after key change from master
                                        await redis_instance._save_rdb()
                                    
                                    # Add this command's bytes to the offset
                                    self.processed_bytes += command_length
                                    print(f"Updated processed bytes to {self.processed_bytes} after INCR")
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
        """Connect to master server and perform initial handshake"""
        
        if not self.master_host or not self.master_port:
            print("Error: Master host and port not set")
            return None, None
        
        try:
            print(f"Connecting to master at {self.master_host}:{self.master_port}")
            
            reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
            
            # Make sure the replica port is set (not None)
            if self.replica_port is None:
                # Default to the "local port" of the connection if we can detect it
                sockname = writer.get_extra_info('sockname')
                if sockname and len(sockname) > 1:
                    self.replica_port = sockname[1]
                else:
                    # If we can't get the local port, use a fixed port of 6380
                    self.replica_port = 6380
                
            print(f"Using replica port: {self.replica_port}")
            
            # Send PING to check connection
            ping_cmd = RESPProtocol.encode_array(["PING"])
            writer.write(ping_cmd)
            
            # Expect PONG response
            response = await reader.readuntil(b"\r\n")
            if not response.startswith(b"+PONG"):
                print(f"Error: Expected PONG, got {response.decode().strip()}")
                writer.close()
                return None, None
            
            # Handshake step 1: Send listening port
            replconf_port = RESPProtocol.encode_array(["REPLCONF", "listening-port", str(self.replica_port)])
            writer.write(replconf_port)
            
            # Expect OK response
            response = await reader.readuntil(b"\r\n")
            if not response.startswith(b"+OK"):
                print(f"Error: Expected OK for REPLCONF listening-port, got {response.decode().strip()}")
                writer.close()
                return None, None
            
            # Continue with rest of handshake...
            
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
                
                # Initialize a variable to collect the complete RDB data
                complete_rdb_data = bytearray()
                
                # Check if REPLCONF command starts immediately after RDB
                remaining_data = b""
                if len(rdb_data) > rdb_content_end and rdb_data[rdb_content_end:].startswith(b"*"):
                    remaining_data = rdb_data[rdb_content_end:]
                    complete_rdb_data.extend(rdb_data[rdb_content_start:rdb_content_end])
                else:
                    # We have partial RDB data
                    complete_rdb_data.extend(rdb_data[rdb_content_start:])
                    
                    # Continue reading RDB content if needed
                    remaining_bytes = rdb_length - len(rdb_data[rdb_content_start:])
                    while remaining_bytes > 0:
                        chunk = await reader.read(min(4096, remaining_bytes))
                        if not chunk:
                            raise ValueError("Connection closed while reading RDB")
                            
                        if len(chunk) > remaining_bytes:
                            # We got more than needed, which might include commands
                            complete_rdb_data.extend(chunk[:remaining_bytes])
                            remaining_data = chunk[remaining_bytes:]
                            remaining_bytes = 0
                        else:
                            complete_rdb_data.extend(chunk)
                            remaining_bytes -= len(chunk)
                
                print(f"Received complete RDB file of length {len(complete_rdb_data)}")
                
                # Now that we have the complete RDB data, we need to load it into our Redis instance
                from redis_server import Redis
                for obj in gc.get_objects():
                    if isinstance(obj, Redis) and obj.replication is self:
                        redis_instance = obj
                        break
                
                if redis_instance:
                    # Create a temporary file to store the RDB data
                    import tempfile
                    import os
                    
                    # Create a temporary directory
                    with tempfile.TemporaryDirectory() as temp_dir:
                        temp_rdb_path = os.path.join(temp_dir, "temp.rdb")
                        
                        # Write the RDB data to the file
                        with open(temp_rdb_path, 'wb') as f:
                            f.write(complete_rdb_data)
                        
                        # Use the RDBParser to load the data
                        from rdb import RDBParser
                        parser = RDBParser()
                        data = parser.load_rdb(temp_dir, "temp.rdb")
                        
                        # Update the Redis instance's data store with the loaded data
                        redis_instance.data_store.update(data)
                        print(f"Loaded {len(data)} keys from master's RDB snapshot")
                        
                        # Save the loaded data to our own RDB file
                        await redis_instance._save_rdb()
                        print("Saved master data to local RDB file")
                
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
                
                # After RDB synchronization, explicitly request all keys from master
                # to make sure we have everything
                print("Requesting all keys from master for verification...")
                keys_cmd = RESPProtocol.encode_array(["KEYS", "*"])
                writer.write(keys_cmd)
                await writer.drain()
                
                # Wait a bit to make sure KEYS command is processed and any 
                # outstanding data is received
                await asyncio.sleep(0.5)
                
                # Process any data that might have been sent in response
                try:
                    extra_data = await asyncio.wait_for(reader.read(4096), 1.0)
                    if extra_data:
                        print(f"Received extra data after synchronization: {len(extra_data)} bytes")
                        
                        # Find Redis instance again to ensure we have the right one
                        from redis_server import Redis
                        redis_instance = None
                        for obj in gc.get_objects():
                            if isinstance(obj, Redis) and obj.replication is self:
                                redis_instance = obj
                                break
                        
                        if redis_instance:
                            # Log current keys in our store
                            current_keys = list(redis_instance.data_store.keys())
                            print(f"Current keys in slave: {current_keys}")
                            
                            # We also want to run a verification - get all keys from master directly
                            # through a separate connection to compare
                            print("Connecting to master directly to verify keys...")
                            try:
                                verify_reader, verify_writer = await asyncio.open_connection(
                                    self.master_host, self.master_port)
                                
                                # Send KEYS * command
                                verify_writer.write(RESPProtocol.encode_array(["KEYS", "*"]))
                                await verify_writer.drain()
                                
                                # Read the response - this will be an array of keys
                                response_data = await verify_reader.read(4096)
                                print(f"Verification data received: {len(response_data)} bytes")
                                
                                # Parse array response to extract keys
                                master_keys = []
                                
                                # Simple parsing of RESP array
                                if response_data.startswith(b"*"):
                                    try:
                                        lines = response_data.split(b"\r\n")
                                        array_size = int(lines[0][1:])
                                        print(f"Master has {array_size} keys")
                                        
                                        # Extract each key
                                        index = 1
                                        for i in range(array_size):
                                            if index + 1 < len(lines) and lines[index].startswith(b"$"):
                                                key = lines[index+1].decode("utf-8")
                                                master_keys.append(key)
                                                index += 2
                                    except Exception as e:
                                        print(f"Error parsing verification response: {e}")
                                
                                print(f"Master keys: {master_keys}")
                                
                                # For any keys in master that aren't in slave, get their values
                                missing_keys = [k for k in master_keys if k not in current_keys]
                                print(f"Keys missing in slave: {missing_keys}")
                                
                                # Fetch and set each missing key
                                for key in missing_keys:
                                    verify_writer.write(RESPProtocol.encode_array(["GET", key]))
                                    await verify_writer.drain()
                                    
                                    # Simple read for the GET response
                                    get_response = await verify_reader.read(1024)
                                    
                                    if get_response.startswith(b"$"):
                                        # It's a bulk string response
                                        lines = get_response.split(b"\r\n")
                                        if len(lines) >= 2 and lines[0].startswith(b"$"):
                                            value_len = int(lines[0][1:])
                                            if value_len > 0:
                                                value = lines[1].decode("utf-8")
                                                # Store it in our data store
                                                redis_instance.data_store[key] = (value, None)
                                                print(f"Added missing key: {key} = {value}")
                                
                                # After adding all missing keys, save to RDB
                                if missing_keys:
                                    await redis_instance._save_rdb()
                                    print("Saved missing keys to local RDB file")
                                
                                # Clean up verification connection
                                verify_writer.close()
                                await verify_writer.wait_closed()
                                
                            except Exception as e:
                                print(f"Error in key verification: {e}")
                except asyncio.TimeoutError:
                    print("No extra data received after synchronization.")
                
                print("Master-slave synchronization complete!")
                return reader, writer
                
            except (ValueError, IndexError) as e:
                print(f"Error processing RDB: {e}")
                writer.close()
                return None, None
            
        except Exception as e:
            print(f"Failed to connect to master: {e}")
            return None, None

    async def handle_replconf(self, args: list, writer: StreamWriter) -> None:
        """Handle REPLCONF command from client"""
        if not args:
            writer.write(RESPProtocol.encode_error("wrong number of arguments for 'replconf' command"))
            return
        
        subcmd = args[0].upper()
        print(f"Received REPLCONF with args: {args}")
        
        if subcmd == "LISTENING-PORT" and len(args) >= 2:
            # Replica is informing us of its listening port
            try:
                port = int(args[1])
                self.replica_port = port
                # Add writer to replicas set
                self.replicas.add(writer)
                writer.write(RESPProtocol.encode_simple_string("OK"))
            except ValueError:
                writer.write(RESPProtocol.encode_error("Invalid port number"))
        
        elif subcmd == "CAPA" and len(args) >= 2:
            # Replica is informing us of its capabilities
            writer.write(RESPProtocol.encode_simple_string("OK"))
        
        elif subcmd == "GETACK" and len(args) >= 2:
            # Master is requesting acknowledgment of processed commands
            # We need to respond with the current processed bytes
            ack_response = RESPProtocol.encode_array(["REPLCONF", "ACK", str(self.processed_bytes)])
            writer.write(ack_response)
        
        elif subcmd == "ACK" and len(args) >= 2:
            # Replica is acknowledging processed commands
            try:
                offset = int(args[1])
                print(f"Received ACK for offset: {offset}")
                # Update the tracking of acknowledged offsets
                self.replica_ack_offsets[writer] = offset
            except ValueError:
                print(f"Invalid offset in ACK: {args[1]}")
        
        else:
            # Default response for other REPLCONF commands
            writer.write(RESPProtocol.encode_simple_string("OK"))

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

    def count_acked_replicas(self, offset: int) -> int:
        """Count how many replicas have acknowledged up to the given offset"""
        count = 0
        for replica, acked_offset in self.replica_ack_offsets.items():
            if not replica.is_closing() and acked_offset >= offset:
                count += 1
        return count
        
    def cleanup_replicas(self) -> None:
        """Remove closed connections from replicas set"""
        closed_replicas = {w for w in self.replicas if w.is_closing()}
        
        # Remove from replicas set
        self.replicas -= closed_replicas
        
        # Remove from ack tracking
        for replica in closed_replicas:
            if replica in self.replica_ack_offsets:
                del self.replica_ack_offsets[replica]

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