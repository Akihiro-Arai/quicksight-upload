import os
from typing import Any, Optional
from dotenv import load_dotenv


class Config:
    def __init__(self, env_file: Optional[str] = None):
        self._config = {}
        
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
        
        self._config = dict(os.environ)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)
    
    def get_required(self, key: str) -> Any:
        value = self.get(key)
        if value is None:
            raise ValueError(f"Required configuration key '{key}' not found")
        return value