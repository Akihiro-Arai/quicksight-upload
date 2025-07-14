import pytest
from unittest.mock import Mock, patch, MagicMock
from src.dashboard_export.quicksight_client import QuickSightClient


class TestQuickSightClient:
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_init(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1')
        
        assert client.account_id == '123456789012'
        assert client.namespace == 'default'
        assert client.folder_path is None
        assert client.quicksight == mock_qs_client
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_list_dashboards(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.list_dashboards.return_value = {
            'DashboardSummaryList': [
                {'DashboardId': 'dash-001', 'Name': 'Dashboard 1'},
                {'DashboardId': 'dash-002', 'Name': 'Dashboard 2'}
            ]
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1')
        dashboards = client.list_dashboards()
        
        assert len(dashboards) == 2
        assert dashboards[0]['DashboardId'] == 'dash-001'
        assert dashboards[1]['DashboardId'] == 'dash-002'
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_list_dashboards_pagination(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_qs_client.list_dashboards.side_effect = [
            {
                'DashboardSummaryList': [
                    {'DashboardId': 'dash-001', 'Name': 'Dashboard 1'}
                ],
                'NextToken': 'token1'
            },
            {
                'DashboardSummaryList': [
                    {'DashboardId': 'dash-002', 'Name': 'Dashboard 2'}
                ]
            }
        ]
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1')
        dashboards = client.list_dashboards()
        
        assert len(dashboards) == 2
        assert mock_qs_client.list_dashboards.call_count == 2
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_get_dashboard_definition(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_definition = {
            'DashboardId': 'dash-001',
            'Name': 'Dashboard 1',
            'Version': {
                'VersionNumber': 1
            }
        }
        mock_qs_client.describe_dashboard_definition.return_value = {
            'Definition': mock_definition
        }
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1')
        definition = client.get_dashboard_definition('dash-001')
        
        assert definition == mock_definition
        mock_qs_client.describe_dashboard_definition.assert_called_with(
            AwsAccountId='123456789012',
            DashboardId='dash-001'
        )
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_assume_cross_account_role(self, mock_aws_manager):
        mock_aws_manager_instance = mock_aws_manager.return_value
        mock_new_client = Mock()
        mock_aws_manager_instance.get_quicksight_client.side_effect = [
            Mock(),  
            mock_new_client  
        ]
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1')
        client.assume_cross_account_role('987654321098', 'TestRole')
        
        assert client.quicksight == mock_new_client
        mock_aws_manager_instance.get_quicksight_client.assert_called_with(account_id='987654321098')
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_init_with_folder_path(self, mock_aws_manager):
        mock_qs_client = Mock()
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1', 'release/')
        
        assert client.folder_path == 'release/'
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_list_dashboards_from_folder(self, mock_aws_manager):
        mock_qs_client = Mock()
        
        # Mock list_folders response
        mock_qs_client.list_folders.return_value = {
            'FolderSummaryList': [
                {'FolderId': 'folder-001', 'Name': 'release'}
            ]
        }
        
        # Mock list_folder_members response
        mock_qs_client.list_folder_members.return_value = {
            'FolderMemberList': [
                {
                    'MemberId': 'dash-001',
                    'MemberType': 'DASHBOARD',
                    'MemberArn': 'arn:aws:quicksight:region:account:dashboard/dash-001/Dashboard1'
                },
                {
                    'MemberId': 'dash-002',
                    'MemberType': 'DASHBOARD',
                    'MemberArn': 'arn:aws:quicksight:region:account:dashboard/dash-002/Dashboard2'
                }
            ]
        }
        
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1', 'release/')
        dashboards = client.list_dashboards()
        
        assert len(dashboards) == 2
        assert dashboards[0]['DashboardId'] == 'dash-001'
        assert dashboards[1]['DashboardId'] == 'dash-002'
        
        mock_qs_client.list_folders.assert_called_with(AwsAccountId='123456789012')
        mock_qs_client.list_folder_members.assert_called_with(
            AwsAccountId='123456789012',
            FolderId='folder-001',
            MaxResults=100
        )
        
    @patch('src.dashboard_export.quicksight_client.AWSClientManager')
    def test_list_dashboards_folder_not_found(self, mock_aws_manager):
        mock_qs_client = Mock()
        
        # Mock empty list_folders response
        mock_qs_client.list_folders.return_value = {
            'FolderSummaryList': []
        }
        
        mock_aws_manager.return_value.get_quicksight_client.return_value = mock_qs_client
        
        client = QuickSightClient('123456789012', 'default', 'ap-northeast-1', 'nonexistent/')
        dashboards = client.list_dashboards()
        
        assert len(dashboards) == 0
        mock_qs_client.list_folders.assert_called_with(AwsAccountId='123456789012')