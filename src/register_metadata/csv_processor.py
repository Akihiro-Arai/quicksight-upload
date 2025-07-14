import csv
from typing import List, Dict
from datetime import datetime
from src.common.logger import setup_logger


class CSVProcessor:
    def __init__(self):
        self.logger = setup_logger('CSVProcessor')
        
    def load_packages_csv(self, file_path: str) -> List[Dict]:
        packages = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                packages.append(row)
                
        self.logger.info(f'Loaded {len(packages)} packages from {file_path}')
        return packages
        
    def load_dashboards_csv(self, file_path: str) -> List[Dict]:
        dashboards = []
        
        with open(file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                dashboards.append(row)
                
        self.logger.info(f'Loaded {len(dashboards)} dashboards from {file_path}')
        return dashboards
        
    def merge_package_dashboards(self, packages: List[Dict], dashboards: List[Dict]) -> List[Dict]:
        merged = []
        
        for package in packages:
            package_id = package['package_id']
            
            package_dashboards = [
                d for d in dashboards 
                if d['package_id'] == package_id
            ]
            
            merged_package = {
                'package_id': package_id,
                'bizuser_code': package['bizuser_code'],
                'label': package['label'],
                'required': package['required'],
                'delete': package['delete'],
                'dashboards': package_dashboards
            }
            
            merged.append(merged_package)
            
        self.logger.info(f'Merged {len(merged)} packages with dashboards')
        return merged
        
    def generate_categories(self, dashboards: List[Dict]) -> List[Dict]:
        categories = {}
        
        for dashboard in dashboards:
            category = dashboard.get('category', '')
            if category and category not in categories:
                categories[category] = {
                    'category': category,
                    'order': len(categories) + 1
                }
                
        return list(categories.values())
        
    def convert_to_dynamodb_format(self, merged_data: List[Dict]) -> List[Dict]:
        records = []
        
        for package in merged_data:
            all_dashboards = package['dashboards']
            categories = self.generate_categories(all_dashboards)
            
            processed_dashboards = []
            for dashboard in all_dashboards:
                processed_dashboard = {
                    'label': dashboard.get('label', ''),
                    'order': int(dashboard.get('order', 0)) if dashboard.get('order') else 0,
                    'category': dashboard.get('category', ''),
                    'tags': self._parse_tags(dashboard.get('tags', '')),
                    'description': dashboard.get('description', '')
                }
                processed_dashboards.append(processed_dashboard)
            
            timestamp = datetime.now().isoformat()
            
            record = {
                'id': 'B004SL_BI',
                'type': f"PACKAGE_{package['bizuser_code']}_{package['package_id']}",
                'bizuser_code': package['bizuser_code'],
                'package_id': package['package_id'],
                'label': package['label'],
                'required': int(package['required']) if package['required'] else 0,
                'delete': int(package['delete']) if package['delete'] else 0,
                'dashboards': processed_dashboards,
                'categories': categories,
                'create_date': timestamp,
                'update_date': timestamp
            }
            
            records.append(record)
            
        self.logger.info(f'Converted {len(records)} records to DynamoDB format')
        return records
        
    def _parse_tags(self, tags_string: str) -> List[str]:
        if not tags_string:
            return []
        return [tag.strip() for tag in tags_string.split(';') if tag.strip()]