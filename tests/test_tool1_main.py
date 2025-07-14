import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from datetime import datetime
from src.dashboard_export.main import DashboardExporter, main


class TestDashboardExporter:
    @patch('src.dashboard_export.main.AWSClientManager')
    @patch('src.dashboard_export.main.Config')
    def test_init(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_ACCOUNT_ID': '123456789012',
            'QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'EXPORT_DASHBOARD_S3_BUCKET': 'test-bucket',
            'EXPORT_DASHBOARD_S3_PREFIX': 'test-prefix/'
        }[key]
        mock_config_instance.get.return_value = 'release/'
        mock_config.return_value = mock_config_instance
        
        exporter = DashboardExporter()
        
        assert exporter.account_id == '123456789012'
        assert exporter.namespace == 'default'
        assert exporter.region == 'ap-northeast-1'
        assert exporter.s3_bucket == 'test-bucket'
        assert exporter.s3_prefix == 'test-prefix/'
        assert exporter.folder_path == 'release/'
        
    @patch('src.dashboard_export.main.AWSClientManager')
    @patch('src.dashboard_export.main.Config')
    @patch('src.dashboard_export.main.QuickSightClient')
    def test_export_dashboards(self, mock_qs_client_class, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_ACCOUNT_ID': '123456789012',
            'QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'EXPORT_DASHBOARD_S3_BUCKET': 'test-bucket',
            'EXPORT_DASHBOARD_S3_PREFIX': 'test-prefix/'
        }[key]
        mock_config_instance.get.return_value = 'release/'
        mock_config.return_value = mock_config_instance
        
        mock_qs_client = Mock()
        mock_qs_client.list_dashboards.return_value = [
            {'DashboardId': 'dash-001', 'Name': 'Dashboard 1'}
        ]
        mock_qs_client.get_dashboard_definition.return_value = {
            'DashboardId': 'dash-001',
            'Name': 'Dashboard 1'
        }
        mock_qs_client_class.return_value = mock_qs_client
        
        mock_s3_client = Mock()
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        exporter = DashboardExporter()
        timestamp = exporter.export_dashboards()
        
        assert timestamp is not None
        assert mock_s3_client.put_object.call_count == 3  
        
    @patch('src.dashboard_export.main.AWSClientManager')
    @patch('src.dashboard_export.main.Config')
    @patch('src.dashboard_export.main.QuickSightClient')
    def test_save_to_s3(self, mock_qs_client_class, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'AWS_ACCOUNT_ID': '123456789012',
            'QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'EXPORT_DASHBOARD_S3_BUCKET': 'test-bucket',
            'EXPORT_DASHBOARD_S3_PREFIX': 'test-prefix/'
        }[key]
        mock_config_instance.get.return_value = 'release/'
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        exporter = DashboardExporter()
        exporter._save_to_s3('test.json', 'test content', '20240101120000')
        
        mock_s3_client.put_object.assert_called_once_with(
            Bucket='test-bucket',
            Key='test-prefix/20240101120000/test.json',
            Body='test content'
        )


@patch('src.dashboard_export.main.DashboardExporter')
@patch('src.dashboard_export.main.setup_logger')
def test_main_success(mock_setup_logger, mock_exporter_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_exporter = Mock()
    mock_exporter.export_dashboards.return_value = '20240101120000'
    mock_exporter_class.return_value = mock_exporter
    
    main()
    
    mock_exporter.export_dashboards.assert_called_once()
    mock_logger.info.assert_any_call('Dashboard export completed successfully')
    

@patch('src.dashboard_export.main.DashboardExporter')
@patch('src.dashboard_export.main.setup_logger')
def test_main_error(mock_setup_logger, mock_exporter_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_exporter = Mock()
    mock_exporter.export_dashboards.side_effect = Exception('Test error')
    mock_exporter_class.return_value = mock_exporter
    
    with pytest.raises(SystemExit) as exc_info:
        main()
    
    assert exc_info.value.code == 1
    mock_logger.error.assert_called()