import json
import sys
from typing import Dict, List

from src.common.aws_client import AWSClientManager
from src.common.config import Config
from src.common.logger import setup_logger
from src.dashboard_deploy.dashboard_deployer import DashboardDeployer
from src.dashboard_deploy.validator import Validator


class DashboardDeployRunner:
    def __init__(self):
        self.config = Config('.env.intg')
        self.logger = setup_logger('DashboardDeployRunner')
        
        self.account_id = self.config.get_required('TARGET_AWS_ACCOUNT_ID')
        self.namespace = self.config.get_required('TARGET_QUICKSIGHT_NAMESPACE')
        self.region = self.config.get_required('AWS_REGION')
        self.s3_bucket = self.config.get_required('DEPLOY_SOURCE_S3_BUCKET')
        self.s3_prefix = self.config.get_required('DEPLOY_SOURCE_S3_PREFIX')
        self.role_name = self.config.get_required('CROSS_ACCOUNT_ROLE_NAME')
        
        self.aws_manager = AWSClientManager(self.region)
        self.s3_client = self.aws_manager.get_s3_client()
        self.validator = Validator(self.account_id, self.region)
        self.deployer = DashboardDeployer(self.account_id, self.namespace, self.region)
        
    def deploy_dashboards(self) -> bool:
        self.logger.info('Starting dashboard deployment')
        
        latest_folder = self._get_latest_s3_folder()
        if not latest_folder:
            self.logger.error('No dashboard folders found in S3')
            return False
            
        self.logger.info(f'Using latest dashboard folder: {latest_folder}')
        
        dashboard_files = self._get_dashboard_files(latest_folder)
        if not dashboard_files:
            self.logger.error('No dashboard files found')
            return False
            
        self.logger.info(f'Found {len(dashboard_files)} dashboard files')
        
        for dashboard_file in dashboard_files:
            dashboard_id = dashboard_file.replace('.json', '')
            self.logger.info(f'Processing dashboard: {dashboard_id}')
            
            definition = self._load_dashboard_from_s3(dashboard_id, latest_folder)
            if not definition:
                self.logger.error(f'Failed to load dashboard {dashboard_id}')
                return False
                
            if not self._validate_dashboard(definition):
                self.logger.error(f'Dashboard {dashboard_id} failed validation')
                return False
                
            if not self.deployer.deploy_dashboard(definition, dashboard_id):
                self.logger.error(f'Failed to deploy dashboard {dashboard_id}')
                return False
                
        self.logger.info('All dashboards deployed successfully')
        return True
        
    def _get_latest_s3_folder(self) -> str:
        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket,
            Prefix=self.s3_prefix,
            Delimiter='/'
        )
        
        folders = []
        for obj in response.get('Contents', []):
            parts = obj['Key'].replace(self.s3_prefix, '').split('/')
            if len(parts) > 1 and parts[0]:
                folders.append(parts[0])
                
        if not folders:
            return None
            
        folders.sort(reverse=True)
        return folders[0]
        
    def _get_dashboard_files(self, folder: str) -> List[str]:
        prefix = f'{self.s3_prefix}{folder}/dashboards/'
        response = self.s3_client.list_objects_v2(
            Bucket=self.s3_bucket,
            Prefix=prefix
        )
        
        files = []
        for obj in response.get('Contents', []):
            filename = obj['Key'].replace(prefix, '')
            if filename.endswith('.json'):
                files.append(filename)
                
        return files
        
    def _load_dashboard_from_s3(self, dashboard_id: str, folder: str) -> Dict:
        key = f'{self.s3_prefix}{folder}/dashboards/{dashboard_id}.json'
        
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=key
            )
            
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            self.logger.error(f'Failed to load dashboard {dashboard_id}: {str(e)}')
            return None
            
    def _validate_dashboard(self, definition: Dict) -> bool:
        if not self.validator.validate_json_structure(definition):
            return False
            
        if not self.validator.validate_required_fields(definition):
            return False
            
        if not self.validator.validate_data_sources(definition):
            return False
            
        return True


def main():
    logger = setup_logger('main')
    
    try:
        runner = DashboardDeployRunner()
        if runner.deploy_dashboards():
            logger.info('Dashboard deploy completed successfully')
        else:
            logger.error('Dashboard deploy failed')
            sys.exit(1)
    except Exception as e:
        logger.error(f'Dashboard deploy failed: {str(e)}')
        sys.exit(1)


if __name__ == '__main__':
    main()