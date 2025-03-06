import os
import struct
from typing import Dict, Tuple, Optional, List, Any

class RDBParser:
    """Parse Redis RDB file format"""
    
    # RDB file format constants
    MAGIC_STRING = b'REDIS'
    RDB_VERSION = b'0011'  # Redis 7.0
    
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
    TYPE_ZIPMAP = 9
    TYPE_ZIPLIST = 10
    TYPE_INTSET = 11
    TYPE_ZSET_ZIPLIST = 12
    TYPE_HASH_ZIPLIST = 13
    
    def __init__(self):
        self.data: Dict[str, Tuple[str, Optional[int]]] = {}
        self._read_length_pos = 0
    
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
            
            # Verify magic string "REDIS"
            if not data.startswith(self.MAGIC_STRING):
                print(f"Invalid RDB file: missing REDIS magic string")
                return {}
            
            # Skip magic string and version (9 bytes)
            pos = 9
            
            # Process the file until EOF
            current_db = 0
            current_expiry = None
            
            while pos < len(data):
                # Read opcode
                opcode = data[pos]
                pos += 1
                
                if opcode == self.OPCODE_EOF:
                    # End of file
                    break
                    
                elif opcode == self.OPCODE_SELECTDB:
                    # Select database
                    current_db = self._read_length(data, pos)
                    pos = self._read_length_pos
                    print(f"Selected database: {current_db}")
                    
                elif opcode == self.OPCODE_EXPIRETIME_MS:
                    # Read expiry time in milliseconds
                    current_expiry = struct.unpack('<Q', data[pos:pos+8])[0]  # 8-byte unsigned int, little-endian
                    pos += 8
                    print(f"Read expiry time: {current_expiry}ms")
                    
                elif opcode == self.OPCODE_EXPIRETIME:
                    # Read expiry time in seconds, convert to ms
                    current_expiry = struct.unpack('<I', data[pos:pos+4])[0] * 1000  # 4-byte unsigned int, little-endian
                    pos += 4
                    print(f"Read expiry time: {current_expiry}ms (from seconds)")
                    
                elif opcode == self.OPCODE_RESIZEDB:
                    # Database size info
                    db_size = self._read_length(data, pos)
                    pos = self._read_length_pos
                    
                    expires_size = self._read_length(data, pos)
                    pos = self._read_length_pos
                    print(f"Database size: {db_size}, expires: {expires_size}")
                    
                elif opcode == self.OPCODE_AUX:
                    # Auxiliary field (metadata)
                    key_len = self._read_length(data, pos)
                    pos = self._read_length_pos
                    
                    key = data[pos:pos+key_len].decode('utf-8', errors='replace')
                    pos += key_len
                    
                    val_len = self._read_length(data, pos)
                    pos = self._read_length_pos
                    
                    val = data[pos:pos+val_len].decode('utf-8', errors='replace')
                    pos += val_len
                    
                    print(f"Read auxiliary field: {key}={val}")
                    
                elif opcode == self.TYPE_STRING:  # String value type
                    # Read key
                    key_len = self._read_length(data, pos)
                    pos = self._read_length_pos
                    
                    key = data[pos:pos+key_len].decode('utf-8', errors='replace')
                    pos += key_len
                    
                    # Read string value
                    val_len = self._read_length(data, pos)
                    pos = self._read_length_pos
                    
                    val = data[pos:pos+val_len].decode('utf-8', errors='replace')
                    pos += val_len
                    
                    print(f"Read key-value: {key}={val} (expires: {current_expiry})")
                    self.data[key] = (val, current_expiry)
                    
                    # Reset expiry after using it
                    current_expiry = None
                    
                else:
                    # Unknown opcode or unsupported type, skip to the next byte
                    print(f"Encountered unsupported opcode or type: {opcode}")
                    # Since we don't know how to parse this, we'll move forward 1 byte
                    # This might cause issues if we hit the middle of a structure
                    # In a real-world scenario, we'd need better error handling
                    pos += 1
            
            print(f"RDB parsing completed. Found {len(self.data)} keys")
            return self.data
                
        except Exception as e:
            print(f"Error parsing RDB file: {e}")
            import traceback
            traceback.print_exc()
            return {}
    
    def _read_length(self, data: bytes, pos: int) -> int:
        """Read a length-encoded field, return the value and update the position"""
        if pos >= len(data):
            raise ValueError(f"Position {pos} out of bounds (data length: {len(data)})")
            
        first_byte = data[pos]
        pos += 1
        
        # Check the first 2 bits
        fmt_bits = (first_byte & 0xC0) >> 6  # Extract top 2 bits
        
        if fmt_bits == 0:  # 00: 6-bit int
            length = first_byte & 0x3F  # Extract lower 6 bits
        elif fmt_bits == 1:  # 01: 14-bit int
            if pos >= len(data):
                raise ValueError("Unexpected end of data while reading 14-bit length")
            next_byte = data[pos]
            pos += 1
            length = ((first_byte & 0x3F) << 8) | next_byte
        elif fmt_bits == 2:  # 10: 32-bit big-endian int
            if pos + 4 > len(data):
                raise ValueError("Unexpected end of data while reading 32-bit length")
            length = struct.unpack('>I', data[pos:pos+4])[0]  # 4-byte unsigned int, big-endian
            pos += 4
        elif fmt_bits == 3:  # 11: special value
            if first_byte == 0xC0:  # 11000000: 8-bit int
                if pos >= len(data):
                    raise ValueError("Unexpected end of data while reading 8-bit length")
                length = data[pos]
                pos += 1
            elif first_byte == 0xC1:  # 11000001: 16-bit int
                if pos + 2 > len(data):
                    raise ValueError("Unexpected end of data while reading 16-bit length")
                length = struct.unpack('>H', data[pos:pos+2])[0]  # 2-byte unsigned int, big-endian
                pos += 2
            elif first_byte == 0xC2:  # 11000010: 32-bit int
                if pos + 4 > len(data):
                    raise ValueError("Unexpected end of data while reading 32-bit length")
                length = struct.unpack('>I', data[pos:pos+4])[0]  # 4-byte unsigned int, big-endian
                pos += 4
            else:
                raise ValueError(f"Invalid length encoding: {first_byte}")
        else:
            raise ValueError(f"Invalid format bits: {fmt_bits}")
        
        self._read_length_pos = pos  # Store updated position
        return length 