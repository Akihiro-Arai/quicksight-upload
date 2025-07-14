import pytest
from unittest.mock import Mock, patch
from src.dashboard_deploy.validator import Validator


class TestValidator:
    def test_validate_json_structure_valid(self):
        validator = Validator()
        valid_definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1'],
            'Version': {'VersionNumber': 1}
        }
        
        assert validator.validate_json_structure(valid_definition) is True
        
    def test_validate_json_structure_invalid(self):
        validator = Validator()
        invalid_definition = None
        
        assert validator.validate_json_structure(invalid_definition) is False
        
    def test_validate_required_fields_all_present(self):
        validator = Validator()
        definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1']
        }
        
        assert validator.validate_required_fields(definition) is True
        
    def test_validate_required_fields_missing_name(self):
        validator = Validator()
        definition = {
            'DataSetIds': ['dataset1']
        }
        
        assert validator.validate_required_fields(definition) is False
        
    def test_validate_required_fields_missing_datasets(self):
        validator = Validator()
        definition = {
            'Name': 'Test Dashboard'
        }
        
        assert validator.validate_required_fields(definition) is False
        
    @patch('src.dashboard_deploy.validator.AWSClientManager')
    def test_validate_data_sources_all_exist(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_data_set.return_value = {
            'DataSet': {'DataSetId': 'dataset1'}
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        validator = Validator('123456789012', 'ap-northeast-1')
        definition = {
            'DataSetIds': ['dataset1']
        }
        
        assert validator.validate_data_sources(definition) is True
        
    @patch('src.dashboard_deploy.validator.AWSClientManager')
    def test_validate_data_sources_not_exist(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_data_set.side_effect = Exception('DataSet not found')
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        validator = Validator('123456789012', 'ap-northeast-1')
        definition = {
            'DataSetIds': ['dataset1']
        }
        
        assert validator.validate_data_sources(definition) is False