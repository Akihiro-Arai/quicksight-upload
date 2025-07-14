import pytest
from unittest.mock import Mock, patch, MagicMock
import json
from src.dashboard_deploy.main import DashboardDeployRunner, main


class TestDashboardDeployRunner:
    @patch('src.dashboard_deploy.main.AWSClientManager')
    @patch('src.dashboard_deploy.main.Config')
    def test_init(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'TARGET_AWS_ACCOUNT_ID': '123456789012',
            'TARGET_QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'DEPLOY_SOURCE_S3_BUCKET': 'test-bucket',
            'DEPLOY_SOURCE_S3_PREFIX': 'test-prefix/',
            'CROSS_ACCOUNT_ROLE_NAME': 'TestRole'
        }[key]
        mock_config.return_value = mock_config_instance
        
        runner = DashboardDeployRunner()
        
        assert runner.account_id == '123456789012'
        assert runner.namespace == 'default'
        assert runner.region == 'ap-northeast-1'
        assert runner.s3_bucket == 'test-bucket'
        assert runner.s3_prefix == 'test-prefix/'
        assert runner.role_name == 'TestRole'
        
    @patch('src.dashboard_deploy.main.AWSClientManager')
    @patch('src.dashboard_deploy.main.Config')
    def test_get_latest_s3_folder(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'TARGET_AWS_ACCOUNT_ID': '123456789012',
            'TARGET_QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'DEPLOY_SOURCE_S3_BUCKET': 'test-bucket',
            'DEPLOY_SOURCE_S3_PREFIX': 'test-prefix/',
            'CROSS_ACCOUNT_ROLE_NAME': 'TestRole'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'test-prefix/20240101120000/dashboard.json'},
                {'Key': 'test-prefix/20240102120000/dashboard.json'},
                {'Key': 'test-prefix/20240103120000/dashboard.json'}
            ]
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        runner = DashboardDeployRunner()
        folder = runner._get_latest_s3_folder()
        
        assert folder == '20240103120000'
        
    @patch('src.dashboard_deploy.main.AWSClientManager')
    @patch('src.dashboard_deploy.main.Config')
    def test_load_dashboard_from_s3(self, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'TARGET_AWS_ACCOUNT_ID': '123456789012',
            'TARGET_QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'DEPLOY_SOURCE_S3_BUCKET': 'test-bucket',
            'DEPLOY_SOURCE_S3_PREFIX': 'test-prefix/',
            'CROSS_ACCOUNT_ROLE_NAME': 'TestRole'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_definition = {'Name': 'Test Dashboard', 'DataSetIds': ['dataset1']}
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_definition).encode()))
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        runner = DashboardDeployRunner()
        definition = runner._load_dashboard_from_s3('dash-001', '20240101120000')
        
        assert definition == mock_definition
        
    @patch('src.dashboard_deploy.main.AWSClientManager')
    @patch('src.dashboard_deploy.main.Config')
    @patch('src.dashboard_deploy.main.Validator')
    @patch('src.dashboard_deploy.main.DashboardDeployer')
    def test_deploy_dashboards(self, mock_deployer_class, mock_validator_class, mock_config, mock_aws_manager):
        mock_config_instance = Mock()
        mock_config_instance.get_required.side_effect = lambda key: {
            'TARGET_AWS_ACCOUNT_ID': '123456789012',
            'TARGET_QUICKSIGHT_NAMESPACE': 'default',
            'AWS_REGION': 'ap-northeast-1',
            'DEPLOY_SOURCE_S3_BUCKET': 'test-bucket',
            'DEPLOY_SOURCE_S3_PREFIX': 'test-prefix/',
            'CROSS_ACCOUNT_ROLE_NAME': 'TestRole'
        }[key]
        mock_config.return_value = mock_config_instance
        
        mock_s3_client = Mock()
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'test-prefix/20240101120000/dashboard.json'}
                ]
            },
            {
                'Contents': [
                    {'Key': 'test-prefix/20240101120000/dashboards/dash-001.json'}
                ]
            }
        ]
        mock_definition = {'Name': 'Test Dashboard', 'DataSetIds': ['dataset1']}
        mock_s3_client.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=json.dumps(mock_definition).encode()))
        }
        mock_aws_manager.return_value.get_s3_client.return_value = mock_s3_client
        
        mock_validator = Mock()
        mock_validator.validate_json_structure.return_value = True
        mock_validator.validate_required_fields.return_value = True
        mock_validator.validate_data_sources.return_value = True
        mock_validator_class.return_value = mock_validator
        
        mock_deployer = Mock()
        mock_deployer.deploy_dashboard.return_value = True
        mock_deployer_class.return_value = mock_deployer
        
        runner = DashboardDeployRunner()
        result = runner.deploy_dashboards()
        
        assert result is True
        mock_deployer.deploy_dashboard.assert_called_once()


@patch('src.dashboard_deploy.main.DashboardDeployRunner')
@patch('src.dashboard_deploy.main.setup_logger')
def test_main_success(mock_setup_logger, mock_runner_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_runner = Mock()
    mock_runner.deploy_dashboards.return_value = True
    mock_runner_class.return_value = mock_runner
    
    main()
    
    mock_runner.deploy_dashboards.assert_called_once()
    mock_logger.info.assert_any_call('Dashboard deploy completed successfully')
    

@patch('src.dashboard_deploy.main.DashboardDeployRunner')
@patch('src.dashboard_deploy.main.setup_logger')
def test_main_error(mock_setup_logger, mock_runner_class):
    mock_logger = Mock()
    mock_setup_logger.return_value = mock_logger
    
    mock_runner = Mock()
    mock_runner.deploy_dashboards.side_effect = Exception('Test error')
    mock_runner_class.return_value = mock_runner
    
    with pytest.raises(SystemExit) as exc_info:
        main()
    
    assert exc_info.value.code == 1
    mock_logger.error.assert_called()