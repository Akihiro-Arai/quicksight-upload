import pytest
from unittest.mock import Mock, patch
from src.dashboard_deploy.dashboard_deployer import DashboardDeployer


class TestDashboardDeployer:
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_init(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        
        assert deployer.account_id == '123456789012'
        assert deployer.namespace == 'default'
        assert deployer.quicksight == mock_qs_client
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_check_existing_dashboard_exists(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_dashboard.return_value = {
            'Dashboard': {'DashboardId': 'dash-001'}
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        exists = deployer.check_existing_dashboard('dash-001')
        
        assert exists is True
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_check_existing_dashboard_not_exists(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_dashboard.side_effect = Exception('ResourceNotFoundException')
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        exists = deployer.check_existing_dashboard('dash-001')
        
        assert exists is False
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_create_dashboard_success(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.create_dashboard.return_value = {
            'DashboardId': 'dash-001',
            'CreationStatus': 'CREATION_SUCCESSFUL'
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1']
        }
        
        result = deployer.create_dashboard(definition, 'dash-001')
        
        assert result is True
        mock_qs_client.create_dashboard.assert_called_once()
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_update_dashboard_success(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.update_dashboard.return_value = {
            'DashboardId': 'dash-001',
            'Status': 200
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1']
        }
        
        result = deployer.update_dashboard(definition, 'dash-001')
        
        assert result is True
        mock_qs_client.update_dashboard.assert_called_once()
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_deploy_dashboard_create_new(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_dashboard.side_effect = Exception('ResourceNotFoundException')
        mock_qs_client.create_dashboard.return_value = {
            'DashboardId': 'dash-001',
            'CreationStatus': 'CREATION_SUCCESSFUL'
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1']
        }
        
        result = deployer.deploy_dashboard(definition, 'dash-001')
        
        assert result is True
        mock_qs_client.create_dashboard.assert_called_once()
        
    @patch('src.dashboard_deploy.dashboard_deployer.AWSClientManager')
    def test_deploy_dashboard_update_existing(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.describe_dashboard.return_value = {
            'Dashboard': {'DashboardId': 'dash-001'}
        }
        mock_qs_client.update_dashboard.return_value = {
            'DashboardId': 'dash-001',
            'Status': 200
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        deployer = DashboardDeployer('123456789012', 'default', 'ap-northeast-1')
        definition = {
            'Name': 'Test Dashboard',
            'DataSetIds': ['dataset1']
        }
        
        result = deployer.deploy_dashboard(definition, 'dash-001')
        
        assert result is True
        mock_qs_client.update_dashboard.assert_called_once()