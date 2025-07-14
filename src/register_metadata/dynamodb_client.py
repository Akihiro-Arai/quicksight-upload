import time
from typing import Dict, List, Any
from src.common.aws_client import AWSClientManager
from src.common.logger import setup_logger


class DynamoDBClient:
    def __init__(self, table_name: str, region: str):
        self.logger = setup_logger('DynamoDBClient')
        self.table_name = table_name
        self.region = region
        self.aws_manager = AWSClientManager(region)
        self.dynamodb = self.aws_manager.get_dynamodb_client()
        
    def put_metadata_record(self, record: Dict) -> bool:
        try:
            formatted_record = self._format_for_dynamodb(record)
            
            self.dynamodb.put_item(
                TableName=self.table_name,
                Item=formatted_record
            )
            
            self.logger.info(f'Successfully put record: {record.get("type", "unknown")}')
            return True
        except Exception as e:
            self.logger.error(f'Failed to put record: {str(e)}')
            return False
            
    def batch_write_records(self, records: List[Dict]) -> bool:
        try:
            batch_size = 25
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                request_items = {
                    self.table_name: [
                        {
                            'PutRequest': {
                                'Item': self._format_for_dynamodb(record)
                            }
                        }
                        for record in batch
                    ]
                }
                
                self._write_batch_with_retry(request_items)
                
            self.logger.info(f'Successfully wrote {len(records)} records')
            return True
        except Exception as e:
            self.logger.error(f'Failed to batch write records: {str(e)}')
            return False
            
    def _write_batch_with_retry(self, request_items: Dict, retry_count: int = 3):
        for attempt in range(retry_count):
            try:
                response = self.dynamodb.batch_write_item(RequestItems=request_items)
                
                unprocessed_items = response.get('UnprocessedItems', {})
                if not unprocessed_items:
                    return
                    
                if attempt < retry_count - 1:
                    time.sleep(2 ** attempt)
                    request_items = unprocessed_items
                else:
                    raise Exception(f'Failed to process all items after {retry_count} attempts')
                    
            except Exception as e:
                if attempt == retry_count - 1:
                    raise
                time.sleep(2 ** attempt)
                
    def _format_for_dynamodb(self, record: Dict) -> Dict:
        formatted = {}
        
        for key, value in record.items():
            formatted[key] = self._convert_to_dynamodb_type(value)
            
        return formatted
        
    def _convert_to_dynamodb_type(self, value: Any) -> Dict:
        if isinstance(value, str):
            return {'S': value}
        elif isinstance(value, int):
            return {'N': str(value)}
        elif isinstance(value, float):
            return {'N': str(value)}
        elif isinstance(value, bool):
            return {'BOOL': value}
        elif isinstance(value, list):
            return {'L': [self._convert_to_dynamodb_type(item) for item in value]}
        elif isinstance(value, dict):
            return {'M': {k: self._convert_to_dynamodb_type(v) for k, v in value.items()}}
        elif value is None:
            return {'NULL': True}
        else:
            return {'S': str(value)}