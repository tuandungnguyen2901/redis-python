from typing import List, Optional, Union, Tuple

class RESPProtocol:
    """Handle RESP (Redis Serialization Protocol) encoding and decoding"""
    
    @staticmethod
    def encode_simple_string(value: str) -> bytes:
        """Encode simple string: +OK\r\n"""
        return f"+{value}\r\n".encode()
    
    @staticmethod
    def encode_error(value: str) -> bytes:
        """Encode error: -Error message\r\n"""
        return f"-{value}\r\n".encode()
    
    @staticmethod
    def encode_integer(value: int) -> bytes:
        """Encode integer: :1000\r\n"""
        return f":{value}\r\n".encode()
    
    @staticmethod
    def encode_bulk_string(value: Optional[str]) -> bytes:
        """Encode a bulk string in RESP format"""
        if value is None:
            return b"$-1\r\n"  # Null bulk string
        
        # First encode the value to bytes
        value_bytes = value.encode('utf-8')
        
        # Then construct the full response
        length_part = f"${len(value_bytes)}\r\n".encode('utf-8')
        
        # Concatenate parts with proper line endings
        return length_part + value_bytes + b"\r\n"
    
    @staticmethod
    def encode_array(value: List[str]) -> bytes:
        """Encode array: *2\r\n$4\r\nBULK\r\n$4\r\nBULK\r\n"""
        array = f"*{len(value)}\r\n"
        for item in value:
            array += f"${len(item)}\r\n{item}\r\n"
        return array.encode()
    
    @staticmethod
    def parse_command(message: str, remaining: Optional[list] = None) -> Tuple[Optional[str], List[str]]:
        """Parse RESP command from message and remaining buffer, return (command, args)"""
        lines = [message]
        if remaining and remaining[0]:
            lines.extend(remaining[0].split('\r\n'))
        
        if not lines[0].startswith('*'):
            # Simple command format
            parts = lines[0].strip().split()
            command = parts[0].upper() if parts else None
            args = parts[1:] if len(parts) > 1 else []
            return command, args
            
        try:
            array_length = int(lines[0][1:])
            expected_lines = 2 + (2 * array_length)
            
            if len(lines) < expected_lines:
                return None, []
                
            command = None
            args = []
            item_index = 1
            
            for i in range(array_length):
                if lines[item_index].startswith('$'):
                    item_index += 1
                    if command is None:
                        command = lines[item_index].upper()
                    else:
                        args.append(lines[item_index])
                    item_index += 1
                    
            return command, args
            
        except (ValueError, IndexError):
            return None, [] 