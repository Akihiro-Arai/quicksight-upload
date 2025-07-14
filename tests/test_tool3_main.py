import pytest
from unittest.mock import Mock, patch, mock_open
from src.register_metadata.main import MetadataRegistrar, main


class TestMetadataRegistrar:
    @patch('src.register_metadata.main.AWSClientManager')
    @patch('src.register_metadata.main.Config')
    def test_init(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_REGION': 'ap-northeast-1',
            'METADATA_SOURCE_S3_BUCKET': 'test-bucket',
            'METADATA_SOURCE_S3_PREFIX': 'test-prefix/',
            'DYNAMODB_TABLE_NAME': 'test-table'
        }[key]
        mock_config.return_value = mock_config_instance
        
        registrar = MetadataRegistrar()
        
        assert registrar.region == 'ap-northeast-1'
        assert registrar.s3_bucket == 'test-bucket'
        assert registrar.s3_prefix == 'test-prefix/'
        assert registrar.table_name == 'test-table'
        
    @patch('src.register_metadata.main.AWSClientManager')
    @patch('src.register_metadata.main.Config')
    def test_get_latest_s3_folder(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_REGION': 'ap-northeast-1',
            'METADATA_SOURCE_S3_BUCKET': 'test-bucket',
            'METADATA_SOURCE_S3_PREFIX': 'test-prefix/',
            'DYNAMODB_TABLE_NAME': 'test-table'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test-prefix/20240101120000/packages.csv'},
                {'Key': 'test-prefix/20240102120000/packages.csv'},
                {'Key': 'test-prefix/20240103120000/packages.csv'}
            ]
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        registrar = MetadataRegistrar()
        folder = registrar._get_latest_s3_folder()
        
        assert folder == '20240103120000'
        
    @patch('src.register_metadata.main.AWSClientManager')
    @patch('src.register_metadata.main.Config')
    def test_download_csv_from_s3(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_REGION': 'ap-northeast-1',
            'METADATA_SOURCE_S3_BUCKET': 'test-bucket',
            'METADATA_SOURCE_S3_PREFIX': 'test-prefix/',
            'DYNAMODB_TABLE_NAME': 'test-table'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        csv_content = "package_id,bizuser_code\nPKG001,BU001"
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=csv_content.encode()))
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        registrar = MetadataRegistrar()
        
        with patch('builtins.open', mock_open()) as mock_file:
            registrar._download_csv_from_s3('packages.csv', '20240101120000', '/tmp/packages.csv')
            
        mock_file.assert_called_once_with('/tmp/packages.csv', 'w', encoding='utf-8')
        
    @patch('src.register_metadata.main.AWSClientManager')
    @patch('src.register_metadata.main.Config')
    @patch('src.register_metadata.main.CSVProcessor')
    @patch('src.register_metadata.main.DynamoDBClient')
    @patch('tempfile.mkdtemp')
    @patch('shutil.rmtree')
    def test_register_metadata(self, mock_rmtree, mock_mkdtemp, mock_dynamodb_class, mock_csv_processor_class, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_REGION': 'ap-northeast-1',
            'METADATA_SOURCE_S3_BUCKET': 'test-bucket',
            'METADATA_SOURCE_S3_PREFIX': 'test-prefix/',
            'DYNAMODB_TABLE_NAME': 'test-table'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test-prefix/20240101120000/packages.csv'}
            ]
        }
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=b'test content'))
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        mock_mkdtemp.return_value = '/tmp/test'
        
        mock_processor = Mock()
        mock_processor.load_packages_csv.return_value = [{'package_id': 'PKG001'}]
        mock_processor.load_dashboards_csv.return_value = [{'dashboard_id': 'dash-001'}]
        mock_processor.merge_package_dashboards.return_value = [{'package_id': 'PKG001', 'dashboards': []}]
        mock_processor.convert_to_dynamodb_format.return_value = [{'id': 'B004SL_BI', 'type': 'PACKAGE_BU001_PKG001'}]
        mock_csv_processor_class.return_value = mock_processor
        
        mock_dynamodb_client = Mock()
        mock_dynamodb_client.batch_write_records.return_value = True
        mock_dynamodb_class.return_value = mock_dynamodb_client
        
        registrar = MetadataRegistrar()
        
        with patch('builtins.open', mock_open()):
            result = registrar.register_metadata()
            
        assert result is True
        mock_dynamodb_client.batch_write_records.assert_called_once()


@patch('src.register_metadata.main.MetadataRegistrar')
@patch('src.register_metadata.main.setup_logger')
def test_main_success(mock_setup_logger, mock_registrar_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_registrar = Mock()
    mock_registrar.register_metadata.return_value = True
    mock_registrar_class.return_value = mock_registrar
    
    main()
    
    mock_registrar.register_metadata.assert_called_once()
    mock_logger.info.assert_any_call('Metadata registration completed successfully')
    

@patch('src.register_metadata.main.MetadataRegistrar')
@patch('src.register_metadata.main.setup_logger')
def test_main_error(mock_setup_logger, mock_registrar_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_registrar = Mock()
    mock_registrar.register_metadata.side_effect = Exception('Test error')
    mock_registrar_class.return_value = mock_registrar
    
    with pytest.raises(SystemExit) as exc_info:
        main()
    
    assert exc_info.value.code == 1
    mock_logger.error.assert_called()