from typing import Dict, Any, List, Optional

class Config:
    """Handles Redis server configuration"""
    
    def __init__(self):
        # Default configuration values
        self._config: Dict[str, Any] = {
            "dir": ".",
            "dbfilename": "dump.rdb",
        }
    
    def set(self, param: str, value: Any) -> None:
        """Set configuration parameter"""
        self._config[param.lower()] = value
    
    def get(self, param: str) -> Any:
        """Get configuration parameter value"""
        return self._config.get(param.lower())
    
    def get_multiple(self, params: List[str]) -> Dict[str, Any]:
        """Get multiple configuration parameters"""
        return {param.lower(): self.get(param) for param in params} 