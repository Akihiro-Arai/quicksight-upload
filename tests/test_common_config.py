import pytest
import os
from unittest.mock import patch, mock_open
from src.common.config import Config


class TestConfig:
    def test_init_with_env_file(self, tmp_path):
        env_file = tmp_path / ".env.test"
        env_file.write_text("TEST_KEY=test_value\nANOTHER_KEY=another_value")
        
        config = Config(str(env_file))
        
        assert config.get("TEST_KEY") == "test_value"
        assert config.get("ANOTHER_KEY") == "another_value"
        
    def test_get_with_default(self):
        config = Config(None)
        
        assert config.get("NON_EXISTENT_KEY", "default") == "default"
        
    def test_get_required_raises_error(self):
        config = Config(None)
        
        with pytest.raises(ValueError, match="Required configuration key 'REQUIRED_KEY' not found"):
            config.get_required("REQUIRED_KEY")
            
    def test_get_required_success(self, tmp_path):
        env_file = tmp_path / ".env.test"
        env_file.write_text("REQUIRED_KEY=required_value")
        
        config = Config(str(env_file))
        
        assert config.get_required("REQUIRED_KEY") == "required_value"
        
    def test_env_override(self, tmp_path):
        env_file = tmp_path / ".env.test"
        env_file.write_text("TEST_KEY=file_value")
        
        with patch.dict(os.environ, {"TEST_KEY": "env_value"}):
            config = Config(str(env_file))
            assert config.get("TEST_KEY") == "env_value"
            
    def test_non_existent_env_file(self):
        config = Config("non_existent.env")
        
        assert config.get("ANY_KEY") is None