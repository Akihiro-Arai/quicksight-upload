from typing import Dict
from src.common.aws_client import AWSClientManager
from src.common.logger import setup_logger


class Validator:
    def __init__(self, account_id: str = None, region: str = 'ap-northeast-1'):
        self.logger = setup_logger('Validator')
        self.account_id = account_id
        self.region = region
        if account_id:
            self.aws_manager = AWSClientManager(region)
            self.quicksight = self.aws_manager.get_quicksight_client()
        
    def validate_json_structure(self, definition: Dict) -> bool:
        if not definition or not isinstance(definition, dict):
            self.logger.error('Invalid JSON structure: definition is None or not a dictionary')
            return False
        return True
        
    def validate_required_fields(self, definition: Dict) -> bool:
        required_fields = ['Name', 'DataSetIds']
        
        for field in required_fields:
            if field not in definition:
                self.logger.error(f'Required field missing: {field}')
                return False
                
        if not definition.get('DataSetIds'):
            self.logger.error('DataSetIds cannot be empty')
            return False
            
        return True
        
    def validate_data_sources(self, definition: Dict) -> bool:
        if not self.account_id:
            self.logger.warning('Cannot validate data sources without account_id')
            return True
            
        dataset_ids = definition.get('DataSetIds', [])
        
        for dataset_id in dataset_ids:
            try:
                self.quicksight.describe_data_set(
                    AwsAccountId=self.account_id,
                    DataSetId=dataset_id
                )
                self.logger.info(f'DataSet {dataset_id} exists')
            except Exception as e:
                self.logger.error(f'DataSet {dataset_id} not found: {str(e)}')
                return False
                
        return True