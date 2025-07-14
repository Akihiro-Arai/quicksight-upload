import pytest
from unittest.mock import Mock, patch, mock_open
import csv
from io import StringIO
from src.register_metadata.csv_processor import CSVProcessor


class TestCSVProcessor:
    def test_load_packages_csv(self):
        csv_content = """package_id,bizuser_code,label,required,delete
PKG001,BU001,Package 1,1,0
PKG002,BU002,Package 2,0,1"""
        
        processor = CSVProcessor()
        
        with patch('builtins.open', mock_open(read_data=csv_content)):
            packages = processor.load_packages_csv('test.csv')
            
        assert len(packages) == 2
        assert packages[0]['package_id'] == 'PKG001'
        assert packages[0]['bizuser_code'] == 'BU001'
        assert packages[0]['required'] == '1'
        assert packages[1]['package_id'] == 'PKG002'
        
    def test_load_dashboards_csv(self):
        csv_content = """package_id,dashboard_id,dashboard_name,label,order,category,tags,description
PKG001,dash-001,Dashboard 1,Label 1,1,sales,tag1;tag2,Description 1
PKG002,dash-002,Dashboard 2,Label 2,2,finance,tag3,Description 2"""
        
        processor = CSVProcessor()
        
        with patch('builtins.open', mock_open(read_data=csv_content)):
            dashboards = processor.load_dashboards_csv('test.csv')
            
        assert len(dashboards) == 2
        assert dashboards[0]['package_id'] == 'PKG001'
        assert dashboards[0]['dashboard_id'] == 'dash-001'
        assert dashboards[0]['tags'] == 'tag1;tag2'
        assert dashboards[1]['package_id'] == 'PKG002'
        
    def test_merge_package_dashboards(self):
        packages = [
            {'package_id': 'PKG001', 'bizuser_code': 'BU001', 'label': 'Package 1', 'required': '1', 'delete': '0'},
            {'package_id': 'PKG002', 'bizuser_code': 'BU002', 'label': 'Package 2', 'required': '0', 'delete': '1'}
        ]
        
        dashboards = [
            {'package_id': 'PKG001', 'dashboard_id': 'dash-001', 'dashboard_name': 'Dashboard 1', 'label': 'Label 1', 'order': '1', 'category': 'sales', 'tags': 'tag1;tag2', 'description': 'Desc 1'},
            {'package_id': 'PKG001', 'dashboard_id': 'dash-002', 'dashboard_name': 'Dashboard 2', 'label': 'Label 2', 'order': '2', 'category': 'finance', 'tags': 'tag3', 'description': 'Desc 2'},
            {'package_id': 'PKG002', 'dashboard_id': 'dash-003', 'dashboard_name': 'Dashboard 3', 'label': 'Label 3', 'order': '1', 'category': 'hr', 'tags': 'tag4', 'description': 'Desc 3'}
        ]
        
        processor = CSVProcessor()
        merged = processor.merge_package_dashboards(packages, dashboards)
        
        assert len(merged) == 2
        assert merged[0]['package_id'] == 'PKG001'
        assert len(merged[0]['dashboards']) == 2
        assert merged[0]['dashboards'][0]['dashboard_id'] == 'dash-001'
        assert merged[1]['package_id'] == 'PKG002'
        assert len(merged[1]['dashboards']) == 1
        
    def test_generate_categories(self):
        dashboards = [
            {'category': 'sales', 'order': '1'},
            {'category': 'finance', 'order': '2'},
            {'category': 'sales', 'order': '3'},
            {'category': 'hr', 'order': '1'}
        ]
        
        processor = CSVProcessor()
        categories = processor.generate_categories(dashboards)
        
        assert len(categories) == 3
        category_names = [cat['category'] for cat in categories]
        assert 'sales' in category_names
        assert 'finance' in category_names
        assert 'hr' in category_names
        
    def test_parse_tags_string(self):
        processor = CSVProcessor()
        
        tags_string = 'tag1;tag2;tag3'
        tags_list = processor._parse_tags(tags_string)
        assert tags_list == ['tag1', 'tag2', 'tag3']
        
        empty_tags = processor._parse_tags('')
        assert empty_tags == []
        
        single_tag = processor._parse_tags('single')
        assert single_tag == ['single']
        
    def test_convert_to_dynamodb_format(self):
        merged_data = [
            {
                'package_id': 'PKG001',
                'bizuser_code': 'BU001',
                'label': 'Package 1',
                'required': '1',
                'delete': '0',
                'dashboards': [
                    {
                        'dashboard_id': 'dash-001',
                        'dashboard_name': 'Dashboard 1',
                        'label': 'Label 1',
                        'order': '1',
                        'category': 'sales',
                        'tags': 'tag1;tag2',
                        'description': 'Desc 1'
                    }
                ]
            }
        ]
        
        processor = CSVProcessor()
        
        with patch.object(processor, 'generate_categories', return_value=[{'category': 'sales', 'order': 1}]):
            result = processor.convert_to_dynamodb_format(merged_data)
            
        assert len(result) == 1
        record = result[0]
        assert record['id'] == 'B004SL_BI'
        assert record['type'] == 'PACKAGE_BU001_PKG001'
        assert record['package_id'] == 'PKG001'
        assert record['bizuser_code'] == 'BU001'
        assert record['required'] == 1
        assert record['delete'] == 0
        assert len(record['dashboards']) == 1
        assert record['dashboards'][0]['label'] == 'Label 1'
        assert record['dashboards'][0]['tags'] == ['tag1', 'tag2']
        assert len(record['categories']) == 1