from typing import List, Dict
from src.common.aws_client import AWSClientManager


class QuickSightClient:
    def __init__(self, account_id: str, namespace: str, region: str, folder_path: str = None):
        self.account_id = account_id
        self.namespace = namespace
        self.region = region
        self.folder_path = folder_path
        self.aws_manager = AWSClientManager(region)
        self.quicksight = self.aws_manager.get_quicksight_client()
        
    def list_dashboards(self) -> List[Dict]:
        if self.folder_path:
            return self._list_dashboards_from_folder()
        else:
            return self._list_all_dashboards()
    
    def _list_all_dashboards(self) -> List[Dict]:
        dashboards = []
        next_token = None
        
        while True:
            params = {
                'AwsAccountId': self.account_id,
                'MaxResults': 100
            }
            
            if next_token:
                params['NextToken'] = next_token
                
            response = self.quicksight.list_dashboards(**params)
            dashboards.extend(response.get('DashboardSummaryList', []))
            
            next_token = response.get('NextToken')
            if not next_token:
                break
                
        return dashboards
    
    def _list_dashboards_from_folder(self) -> List[Dict]:
        folder_id = self._get_folder_id_by_path(self.folder_path)
        if not folder_id:
            return []
            
        dashboards = []
        next_token = None
        
        while True:
            params = {
                'AwsAccountId': self.account_id,
                'FolderId': folder_id,
                'MaxResults': 100
            }
            
            if next_token:
                params['NextToken'] = next_token
                
            response = self.quicksight.list_folder_members(**params)
            folder_members = response.get('FolderMemberList', [])
            
            # Filter for dashboards only
            dashboard_members = [
                member for member in folder_members 
                if member.get('MemberType') == 'DASHBOARD'
            ]
            
            for member in dashboard_members:
                dashboard_summary = {
                    'DashboardId': member['MemberId'],
                    'Name': member.get('MemberArn', '').split('/')[-1]  # Extract name from ARN
                }
                dashboards.append(dashboard_summary)
            
            next_token = response.get('NextToken')
            if not next_token:
                break
                
        return dashboards
    
    def _get_folder_id_by_path(self, folder_path: str) -> str:
        # Remove trailing slash if present
        folder_path = folder_path.rstrip('/')
        
        try:
            # List all folders and find the one matching the path
            response = self.quicksight.list_folders(
                AwsAccountId=self.account_id
            )
            
            folders = response.get('FolderSummaryList', [])
            for folder in folders:
                if folder.get('Name') == folder_path:
                    return folder.get('FolderId')
                    
        except Exception as e:
            # If folder listing fails, return None to fall back to all dashboards
            return None
            
        return None
        
    def get_dashboard_definition(self, dashboard_id: str) -> Dict:
        response = self.quicksight.describe_dashboard_definition(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id
        )
        return response['Definition']
        
    def assume_cross_account_role(self, account_id: str, role_name: str):
        self.quicksight = self.aws_manager.get_quicksight_client(account_id=account_id)