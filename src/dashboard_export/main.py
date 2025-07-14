import json
import sys
from datetime import datetime
from typing import List, Dict

from src.common.aws_client import AWSClientManager
from src.common.config import Config
from src.common.logger import setup_logger
from src.dashboard_export.quicksight_client import QuickSightClient
from src.dashboard_export.csv_generator import CSVGenerator


class DashboardExporter:
    def __init__(self):
        self.config = Config('.env.dev2')
        self.logger = setup_logger('DashboardExporter')
        
        self.account_id = self.config.get_required('AWS_ACCOUNT_ID')
        self.namespace = self.config.get_required('QUICKSIGHT_NAMESPACE')
        self.region = self.config.get_required('AWS_REGION')
        self.s3_bucket = self.config.get_required('EXPORT_DASHBOARD_S3_BUCKET')
        self.s3_prefix = self.config.get_required('EXPORT_DASHBOARD_S3_PREFIX')
        self.folder_path = self.config.get('QUICKSIGHT_FOLDER_PATH')
        
        self.aws_manager = AWSClientManager(self.region)
        self.s3_client = self.aws_manager.get_s3_client()
        self.quicksight_client = QuickSightClient(
            self.account_id, self.namespace, self.region, self.folder_path
        )
        self.csv_generator = CSVGenerator()
        
    def export_dashboards(self) -> str:
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        self.logger.info(f'Starting dashboard export with timestamp: {timestamp}')
        
        dashboards = self.quicksight_client.list_dashboards()
        folder_info = f' in folder "{self.folder_path}"' if self.folder_path else ''
        self.logger.info(f'Found {len(dashboards)} dashboards{folder_info}')
        
        for dashboard in dashboards:
            dashboard_id = dashboard['DashboardId']
            self.logger.info(f'Exporting dashboard: {dashboard_id}')
            
            definition = self.quicksight_client.get_dashboard_definition(dashboard_id)
            
            filename = f'dashboards/{dashboard_id}.json'
            self._save_to_s3(filename, json.dumps(definition, indent=2), timestamp)
            
        packages_csv = self.csv_generator.generate_packages_csv(dashboards)
        self._save_to_s3('packages.csv', packages_csv, timestamp)
        
        dashboards_csv = self.csv_generator.generate_dashboards_csv(dashboards)
        self._save_to_s3('dashboards.csv', dashboards_csv, timestamp)
        
        self.logger.info('Dashboard export completed')
        return timestamp
        
    def _save_to_s3(self, filename: str, content: str, timestamp: str):
        key = f'{self.s3_prefix}{timestamp}/{filename}'
        self.logger.info(f'Saving to S3: s3://{self.s3_bucket}/{key}')
        
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=content
        )


def main():
    logger = setup_logger('main')
    
    try:
        exporter = DashboardExporter()
        exporter.export_dashboards()
        logger.info('Dashboard export completed successfully')
    except Exception as e:
        logger.error(f'Dashboard export failed: {str(e)}')
        sys.exit(1)


if __name__ == '__main__':
    main()