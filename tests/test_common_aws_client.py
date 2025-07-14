import pytest
from unittest.mock import Mock, patch, MagicMock
import boto3
from src.common.aws_client import AWSClientManager


class TestAWSClientManager:
    @patch('boto3.client')
    def test_init_default_region(self, mock_boto_client):
        manager = AWSClientManager()
        
        assert manager.region == 'ap-northeast-1'
        
    @patch('boto3.client')
    def test_init_custom_region(self, mock_boto_client):
        manager = AWSClientManager(region='us-east-1')
        
        assert manager.region == 'us-east-1'
        
    @patch('boto3.client')
    def test_get_quicksight_client_default(self, mock_boto_client):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        manager = AWSClientManager()
        client = manager.get_quicksight_client()
        
        mock_boto_client.assert_called_with('quicksight', region_name='ap-northeast-1')
        assert client == mock_client
        
    @patch('boto3.client')
    def test_get_quicksight_client_with_account_id(self, mock_boto_client):
        mock_sts_client = Mock()
        mock_qs_client = Mock()
        mock_assume_role_response = {
            'Credentials': {
                'AccessKeyId': 'test_key',
                'SecretAccessKey': 'test_secret',
                'SessionToken': 'test_token'
            }
        }
        mock_sts_client.assume_role.return_value = mock_assume_role_response
        
        def client_side_effect(service, **kwargs):
            if service == 'sts':
                return mock_sts_client
            return mock_qs_client
            
        mock_boto_client.side_effect = client_side_effect
        
        manager = AWSClientManager()
        client = manager.get_quicksight_client(account_id='123456789012')
        
        assert client == mock_qs_client
        
    @patch('boto3.client')
    def test_get_s3_client(self, mock_boto_client):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        manager = AWSClientManager()
        client = manager.get_s3_client()
        
        mock_boto_client.assert_called_with('s3', region_name='ap-northeast-1')
        assert client == mock_client
        
    @patch('boto3.client')
    def test_get_dynamodb_client(self, mock_boto_client):
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        manager = AWSClientManager()
        client = manager.get_dynamodb_client()
        
        mock_boto_client.assert_called_with('dynamodb', region_name='ap-northeast-1')
        assert client == mock_client
        
    @patch('boto3.client')
    def test_assume_role(self, mock_boto_client):
        mock_sts_client = Mock()
        mock_credentials = {
            'Credentials': {
                'AccessKeyId': 'test_key',
                'SecretAccessKey': 'test_secret',
                'SessionToken': 'test_token'
            }
        }
        mock_sts_client.assume_role.return_value = mock_credentials
        mock_boto_client.return_value = mock_sts_client
        
        manager = AWSClientManager()
        result = manager.assume_role('123456789012', 'TestRole')
        
        mock_sts_client.assume_role.assert_called_with(
            RoleArn='arn:aws:iam::123456789012:role/TestRole',
            RoleSessionName='QuickSightManagementTools'
        )
        assert result == mock_credentials['Credentials']