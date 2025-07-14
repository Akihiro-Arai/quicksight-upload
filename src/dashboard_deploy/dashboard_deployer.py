from typing import Dict
from src.common.aws_client import AWSClientManager
from src.common.logger import setup_logger


class DashboardDeployer:
    def __init__(self, account_id: str, namespace: str, region: str):
        self.logger = setup_logger('DashboardDeployer')
        self.account_id = account_id
        self.namespace = namespace
        self.region = region
        self.aws_manager = AWSClientManager(region)
        self.quicksight = self.aws_manager.get_quicksight_client()
        
    def deploy_dashboard(self, definition: Dict, dashboard_id: str) -> bool:
        self.logger.info(f'Deploying dashboard: {dashboard_id}')
        
        if self.check_existing_dashboard(dashboard_id):
            self.logger.info(f'Dashboard {dashboard_id} exists, updating...')
            return self.update_dashboard(definition, dashboard_id)
        else:
            self.logger.info(f'Dashboard {dashboard_id} does not exist, creating...')
            return self.create_dashboard(definition, dashboard_id)
            
    def check_existing_dashboard(self, dashboard_id: str) -> bool:
        try:
            self.quicksight.describe_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id
            )
            return True
        except Exception as e:
            if 'ResourceNotFoundException' in str(e):
                return False
            raise
            
    def create_dashboard(self, definition: Dict, dashboard_id: str) -> bool:
        try:
            response = self.quicksight.create_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name=definition.get('Name', dashboard_id),
                Definition=definition
            )
            
            self.logger.info(f'Dashboard {dashboard_id} created successfully')
            return True
        except Exception as e:
            self.logger.error(f'Failed to create dashboard {dashboard_id}: {str(e)}')
            return False
            
    def update_dashboard(self, definition: Dict, dashboard_id: str) -> bool:
        try:
            response = self.quicksight.update_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name=definition.get('Name', dashboard_id),
                Definition=definition
            )
            
            self.logger.info(f'Dashboard {dashboard_id} updated successfully')
            return True
        except Exception as e:
            self.logger.error(f'Failed to update dashboard {dashboard_id}: {str(e)}')
            return False