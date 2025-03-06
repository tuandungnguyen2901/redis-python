import os
import struct
from typing import Dict, Tuple, Optional, List, Any, Set

class RDBParser:
    """Parse Redis RDB file format"""
    
    # RDB file format constants
    MAGIC_STRING = b'REDIS'
    
    # RDB opcodes
    OPCODE_EOF = 0xFF
    OPCODE_SELECTDB = 0xFE
    OPCODE_EXPIRETIME_MS = 0xFC
    OPCODE_EXPIRETIME = 0xFD
    OPCODE_RESIZEDB = 0xFB
    OPCODE_AUX = 0xFA
    
    # Value types
    TYPE_STRING = 0
    TYPE_LIST = 1
    TYPE_SET = 2
    TYPE_ZSET = 3
    TYPE_HASH = 4
    
    def __init__(self):
        self.data: Dict[str, Tuple[str, Optional[int]]] = {}
        self._pos = 0
    
    def load_rdb(self, dir_path: str, filename: str) -> Dict[str, Tuple[str, Optional[int]]]:
        """Load data from RDB file"""
        filepath = os.path.join(dir_path, filename)
        
        # Check if the file exists
        if not os.path.exists(filepath):
            print(f"RDB file {filepath} does not exist, starting with empty database")
            return {}
        
        try:
            with open(filepath, 'rb') as f:
                data = f.read()
            
            # Reset position and data for a fresh parse
            self._pos = 0
            self.data = {}
            
            # Verify magic string "REDIS"
            if not data.startswith(self.MAGIC_STRING):
                print(f"Invalid RDB file: missing REDIS magic string")
                return {}
            
            # Skip magic string and version (9 bytes total)
            self._pos = 9
            
            current_db = 0
            current_expiry = None
            
            # Parse until EOF
            while self._pos < len(data):
                # Check if we've reached the end of the file
                if self._pos >= len(data):
                    break
                
                # Read opcode
                opcode = data[self._pos]
                self._pos += 1
                
                if opcode == self.OPCODE_EOF:
                    # End of file
                    print("Found EOF marker")
                    break
                    
                elif opcode == self.OPCODE_SELECTDB:
                    # Select database
                    if self._pos < len(data):
                        current_db = self._read_length_encoding(data)
                        print(f"Selected database: {current_db}")
                    else:
                        break
                    
                elif opcode == self.OPCODE_EXPIRETIME_MS:
                    # Read expiry time in milliseconds (8 bytes, little-endian)
                    if self._pos + 8 <= len(data):
                        current_expiry = struct.unpack('<Q', data[self._pos:self._pos+8])[0]
                        self._pos += 8
                        print(f"Read expiry time (ms): {current_expiry}")
                    else:
                        break
                    
                elif opcode == self.OPCODE_EXPIRETIME:
                    # Read expiry time in seconds (4 bytes, little-endian), convert to ms
                    if self._pos + 4 <= len(data):
                        current_expiry = struct.unpack('<I', data[self._pos:self._pos+4])[0] * 1000
                        self._pos += 4
                        print(f"Read expiry time (s->ms): {current_expiry}")
                    else:
                        break
                    
                elif opcode == self.OPCODE_RESIZEDB:
                    # Database size info - two length encoded values
                    try:
                        hash_table_size = self._read_length_encoding(data)
                        expires_size = self._read_length_encoding(data)
                        print(f"Database hash table size: {hash_table_size}, expires: {expires_size}")
                    except ValueError as e:
                        print(f"Error reading RESIZEDB: {e}")
                        break
                    
                elif opcode == self.OPCODE_AUX:
                    # Auxiliary field (metadata)
                    try:
                        key = self._read_string_encoding(data)
                        value = self._read_string_encoding(data)
                        print(f"Read auxiliary field: {key}={value}")
                    except ValueError as e:
                        print(f"Error reading AUX: {e}")
                        break
                
                # Handle key-value pairs
                elif opcode == self.TYPE_STRING:
                    # String type
                    try:
                        key = self._read_string_encoding(data)
                        value = self._read_string_encoding(data)
                        
                        print(f"Read key-value: {key}={value} (expires: {current_expiry})")
                        self.data[key] = (value, current_expiry)
                        
                        # Reset expiry after using it
                        current_expiry = None
                    except ValueError as e:
                        print(f"Error reading string KV: {e}")
                        current_expiry = None
                        # Try to continue parsing
                        
                # Skip other value types for now
                elif opcode >= 1 and opcode <= 4:
                    try:
                        # Read the key
                        key = self._read_string_encoding(data)
                        print(f"Found key with unsupported value type {opcode}: {key}")
                        
                        # For simplicity store just the key with a placeholder value
                        # In a full implementation we would parse the specific value type
                        self.data[key] = (f"<type:{opcode}>", current_expiry)
                        
                        # Skip the value for now (we're not parsing non-string types)
                        # But we record the key for KEYS command
                        
                        # Reset expiry after using it
                        current_expiry = None
                    except ValueError as e:
                        print(f"Error reading key with value type {opcode}: {e}")
                        current_expiry = None
                        # Try to continue parsing

                else:
                    # Unknown or unsupported opcode
                    print(f"Unsupported opcode: {opcode} at position {self._pos-1}")
                    # In a real implementation we would handle all opcodes properly
                    
                    # Try to skip this value and continue
                    current_expiry = None
                    
            print(f"RDB parsing completed. Found {len(self.data)} keys: {', '.join(self.data.keys())}")
            return self.data
                
        except Exception as e:
            print(f"Error parsing RDB file: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _read_length_encoding(self, data: bytes) -> int:
        """Read a length-encoded value as specified in the RDB format"""
        if self._pos >= len(data):
            raise ValueError(f"Position {self._pos} out of bounds (data length: {len(data)})")
            
        # Read the first byte
        first_byte = data[self._pos]
        self._pos += 1
        
        # Extract the first 2 bits to determine the encoding
        # 00, 01, 10, 11 (in binary) = 0, 1, 2, 3 (in decimal)
        first_two_bits = (first_byte & 0xC0) >> 6
        
        if first_two_bits == 0:  # 00: 6-bit integer
            # The remaining 6 bits of the byte (00111111 = 0x3F)
            return first_byte & 0x3F
            
        elif first_two_bits == 1:  # 01: 14-bit integer
            if self._pos >= len(data):
                raise ValueError("Unexpected end of data while reading 14-bit length")
                
            # The next byte combined with the remaining 6 bits of the first byte
            next_byte = data[self._pos]
            self._pos += 1
            return ((first_byte & 0x3F) << 8) | next_byte
            
        elif first_two_bits == 2:  # 10: 32-bit integer (big-endian)
            if self._pos + 4 > len(data):
                raise ValueError("Unexpected end of data while reading 32-bit length")
                
            # The next 4 bytes are the integer (big-endian)
            length = struct.unpack('>I', data[self._pos:self._pos+4])[0]
            self._pos += 4
            return length
            
        elif first_two_bits == 3:  # 11: special encoding
            if first_byte == 0xC0:  # 8-bit integer
                if self._pos >= len(data):
                    raise ValueError("Unexpected end of data while reading 8-bit int")
                    
                value = data[self._pos]
                self._pos += 1
                return value
                
            elif first_byte == 0xC1:  # 16-bit integer (little-endian)
                if self._pos + 2 > len(data):
                    raise ValueError("Unexpected end of data while reading 16-bit int")
                    
                value = struct.unpack('<H', data[self._pos:self._pos+2])[0]
                self._pos += 2
                return value
                
            elif first_byte == 0xC2:  # 32-bit integer (little-endian)
                if self._pos + 4 > len(data):
                    raise ValueError("Unexpected end of data while reading 32-bit int")
                    
                value = struct.unpack('<I', data[self._pos:self._pos+4])[0]
                self._pos += 4
                return value
                
            else:
                # Other special encodings not supported yet
                raise ValueError(f"Unsupported special encoding: {first_byte:02x}")
        
        # Should never reach here
        raise ValueError(f"Invalid length encoding: {first_byte:02x}")
    
    def _read_string_encoding(self, data: bytes) -> str:
        """Read a string-encoded value according to the RDB format"""
        if self._pos >= len(data):
            raise ValueError(f"Position {self._pos} out of bounds (data length: {len(data)})")
            
        # Read the first byte to determine the encoding
        first_byte = data[self._pos]
        
        # Check if it's a special encoding (starting with 0b11 = 0xC0)
        if (first_byte & 0xC0) == 0xC0:
            # Special string encoding
            if first_byte == 0xC0:  # 8-bit integer
                self._pos += 1  # Move past the encoding byte
                if self._pos >= len(data):
                    raise ValueError("Unexpected end of data while reading 8-bit int string")
                    
                value = data[self._pos]
                self._pos += 1
                return str(value)
                
            elif first_byte == 0xC1:  # 16-bit integer
                self._pos += 1  # Move past the encoding byte
                if self._pos + 2 > len(data):
                    raise ValueError("Unexpected end of data while reading 16-bit int string")
                    
                value = struct.unpack('<H', data[self._pos:self._pos+2])[0]
                self._pos += 2
                return str(value)
                
            elif first_byte == 0xC2:  # 32-bit integer
                self._pos += 1  # Move past the encoding byte
                if self._pos + 4 > len(data):
                    raise ValueError("Unexpected end of data while reading 32-bit int string")
                    
                value = struct.unpack('<I', data[self._pos:self._pos+4])[0]
                self._pos += 4
                return str(value)
                
            # Other special encodings (like compressed strings) not supported
            raise ValueError(f"Unsupported special string encoding: {first_byte:02x}")
        
        # Regular string encoding - length followed by data
        try:
            # Read the length
            length = self._read_length_encoding(data)
            
            # Read the string of the specified length
            if self._pos + length > len(data):
                raise ValueError(f"String length {length} exceeds remaining data at position {self._pos}")
                
            string_data = data[self._pos:self._pos+length]
            self._pos += length
            
            # Decode the bytes to a string (utf-8 with error replacement)
            return string_data.decode('utf-8', errors='replace')
            
        except ValueError as e:
            raise ValueError(f"Error reading string: {e}") 