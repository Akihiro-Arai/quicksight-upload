import pytest
from unittest.mock import Mock, patch
from src.register_metadata.dynamodb_client import DynamoDBClient


class TestDynamoDBClient:
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_init(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        assert client.table_name == 'test-table'
        assert client.region == 'ap-northeast-1'
        assert client.dynamodb == mock_db_client
        
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_put_metadata_record_success(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_db_client.put_item.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        record = {
            'id': 'B004SL_BI',
            'type': 'PACKAGE_BU001_PKG001',
            'package_id': 'PKG001',
            'bizuser_code': 'BU001',
            'label': 'Test Package',
            'required': 1,
            'delete': 0,
            'dashboards': [],
            'categories': []
        }
        
        result = client.put_metadata_record(record)
        
        assert result is True
        mock_db_client.put_item.assert_called_once()
        
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_put_metadata_record_failure(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_db_client.put_item.side_effect = Exception('DynamoDB error')
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        record = {
            'id': 'B004SL_BI',
            'type': 'PACKAGE_BU001_PKG001',
            'package_id': 'PKG001'
        }
        
        result = client.put_metadata_record(record)
        
        assert result is False
        
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_batch_write_records_success(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_db_client.batch_write_item.return_value = {
            'UnprocessedItems': {},
            'ResponseMetadata': {'HTTPStatusCode': 200}
        }
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        records = [
            {
                'id': 'B004SL_BI',
                'type': 'PACKAGE_BU001_PKG001',
                'package_id': 'PKG001'
            },
            {
                'id': 'B004SL_BI',
                'type': 'PACKAGE_BU002_PKG002',
                'package_id': 'PKG002'
            }
        ]
        
        result = client.batch_write_records(records)
        
        assert result is True
        mock_db_client.batch_write_item.assert_called_once()
        
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_batch_write_records_with_unprocessed_items(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_db_client.batch_write_item.side_effect = [
            {
                'UnprocessedItems': {
                    'test-table': [
                        {
                            'PutRequest': {
                                'Item': {
                                    'id': {'S': 'B004SL_BI'},
                                    'type': {'S': 'PACKAGE_BU001_PKG001'}
                                }
                            }
                        }
                    ]
                }
            },
            {
                'UnprocessedItems': {},
                'ResponseMetadata': {'HTTPStatusCode': 200}
            }
        ]
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        records = [
            {
                'id': 'B004SL_BI',
                'type': 'PACKAGE_BU001_PKG001',
                'package_id': 'PKG001'
            }
        ]
        
        result = client.batch_write_records(records)
        
        assert result is True
        assert mock_db_client.batch_write_item.call_count == 2
        
    @patch('src.register_metadata.dynamodb_client.AWSClientManager')
    def test_batch_write_records_failure(self, mock_aws_manager):
        mock_db_client = Mock()
        mock_db_client.batch_write_item.side_effect = Exception('DynamoDB error')
        mock_aws_manager.return_value.get_dynamodb_client.return_value = mock_db_client
        
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        records = [
            {
                'id': 'B004SL_BI',
                'type': 'PACKAGE_BU001_PKG001',
                'package_id': 'PKG001'
            }
        ]
        
        result = client.batch_write_records(records)
        
        assert result is False
        
    def test_format_for_dynamodb(self):
        client = DynamoDBClient('test-table', 'ap-northeast-1')
        
        record = {
            'id': 'B004SL_BI',
            'type': 'PACKAGE_BU001_PKG001',
            'package_id': 'PKG001',
            'required': 1,
            'dashboards': [
                {
                    'label': 'Dashboard 1',
                    'order': 1,
                    'tags': ['tag1', 'tag2']
                }
            ],
            'categories': [
                {
                    'category': 'sales',
                    'order': 1
                }
            ]
        }
        
        formatted = client._format_for_dynamodb(record)
        
        assert formatted['id']['S'] == 'B004SL_BI'
        assert formatted['type']['S'] == 'PACKAGE_BU001_PKG001'
        assert formatted['package_id']['S'] == 'PKG001'
        assert formatted['required']['N'] == '1'
        assert len(formatted['dashboards']['L']) == 1
        assert len(formatted['categories']['L']) == 1