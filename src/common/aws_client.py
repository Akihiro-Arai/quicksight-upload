import boto3
from typing import Dict, Optional


class AWSClientManager:
    def __init__(self, region: str = 'ap-northeast-1'):
        self.region = region
        
    def get_quicksight_client(self, account_id: Optional[str] = None):
        if account_id:
            credentials = self.assume_role(account_id, 'QuickSightDeployRole')
            return boto3.client(
                'quicksight',
                region_name=self.region,
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        return boto3.client('quicksight', region_name=self.region)
        
    def get_s3_client(self):
        return boto3.client('s3', region_name=self.region)
        
    def get_dynamodb_client(self):
        return boto3.client('dynamodb', region_name=self.region)
        
    def assume_role(self, account_id: str, role_name: str) -> Dict:
        sts_client = boto3.client('sts', region_name=self.region)
        response = sts_client.assume_role(
            RoleArn=f'arn:aws:iam::{account_id}:role/{role_name}',
            RoleSessionName='QuickSightManagementTools'
        )
        return response['Credentials']