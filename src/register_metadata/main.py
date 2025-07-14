import os
import sys
import tempfile
import shutil
from typing import List, Dict

from src.common.aws_client import AWSClientManager
from src.common.config import Config
from src.common.logger import setup_logger
from src.register_metadata.csv_processor import CSVProcessor
from src.register_metadata.dynamodb_client import DynamoDBClient


class MetadataRegistrar:
    def __init__(self):
        self.config = Config('.env.intg')
        self.logger = setup_logger('MetadataRegistrar')
        
        self.region = self.config.get_required('AWS_REGION')
        self.s3_bucket = self.config.get_required('METADATA_SOURCE_S3_BUCKET')
        self.s3_prefix = self.config.get_required('METADATA_SOURCE_S3_PREFIX')
        self.table_name = self.config.get_required('DYNAMODB_TABLE_NAME')
        
        self.aws_manager = AWSClientManager(self.region)
        self.s3_client = self.aws_manager.get_s3_client()
        self.csv_processor = CSVProcessor()
        self.dynamodb_client = DynamoDBClient(self.table_name, self.region)
        
    def register_metadata(self) -> bool:
        self.logger.info('Starting metadata registration')
        
        latest_folder = self._get_latest_s3_folder()
        if not latest_folder:
            self.logger.error('No metadata folders found in S3')
            return False
            
        self.logger.info(f'Using latest metadata folder: {latest_folder}')
        
        temp_dir = tempfile.mkdtemp()
        try:
            packages_file = os.path.join(temp_dir, 'packages.csv')
            dashboards_file = os.path.join(temp_dir, 'dashboards.csv')
            
            self._download_csv_from_s3('packages.csv', latest_folder, packages_file)
            self._download_csv_from_s3('dashboards.csv', latest_folder, dashboards_file)
            
            packages = self.csv_processor.load_packages_csv(packages_file)
            dashboards = self.csv_processor.load_dashboards_csv(dashboards_file)
            
            merged_data = self.csv_processor.merge_package_dashboards(packages, dashboards)
            
            dynamodb_records = self.csv_processor.convert_to_dynamodb_format(merged_data)
            
            if not self.dynamodb_client.batch_write_records(dynamodb_records):
                self.logger.error('Failed to write records to DynamoDB')
                return False
                
            self.logger.info('Metadata registration completed successfully')
            return True
            
        finally:
            shutil.rmtree(temp_dir)
            
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
        
    def _download_csv_from_s3(self, filename: str, folder: str, local_path: str):
        key = f'{self.s3_prefix}{folder}/{filename}'
        self.logger.info(f'Downloading s3://{self.s3_bucket}/{key} to {local_path}')
        
        response = self.s3_client.get_object(
            Bucket=self.s3_bucket,
            Key=key
        )
        
        with open(local_path, 'w', encoding='utf-8') as f:
            f.write(response['Body'].read().decode('utf-8'))


def main():
    logger = setup_logger('main')
    
    try:
        registrar = MetadataRegistrar()
        if registrar.register_metadata():
            logger.info('Metadata registration completed successfully')
        else:
            logger.error('Metadata registration failed')
            sys.exit(1)
    except Exception as e:
        logger.error(f'Metadata registration failed: {str(e)}')
        sys.exit(1)


if __name__ == '__main__':
    main()