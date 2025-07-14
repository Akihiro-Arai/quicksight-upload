import pytest
from src.dashboard_export.csv_generator import CSVGenerator


class TestCSVGenerator:
    def test_generate_packages_csv_empty(self):
        generator = CSVGenerator()
        csv_content = generator.generate_packages_csv([])
        
        expected = "package_id,bizuser_code,label,required,delete"
        assert csv_content.strip() == expected
        
    def test_generate_packages_csv_with_dashboards(self):
        dashboards = [
            {'DashboardId': 'dash-001', 'Name': 'Dashboard 1'},
            {'DashboardId': 'dash-002', 'Name': 'Dashboard 2'}
        ]
        
        generator = CSVGenerator()
        csv_content = generator.generate_packages_csv(dashboards)
        
        lines = [line.strip() for line in csv_content.strip().split('\n')]
        assert len(lines) == 3  
        assert lines[0] == "package_id,bizuser_code,label,required,delete"
        assert ",,,," in lines[1]
        assert ",,,," in lines[2]
        
    def test_generate_dashboards_csv_empty(self):
        generator = CSVGenerator()
        csv_content = generator.generate_dashboards_csv([])
        
        expected = "package_id,dashboard_id,dashboard_name,label,order,category,tags,description"
        assert csv_content.strip() == expected
        
    def test_generate_dashboards_csv_with_dashboards(self):
        dashboards = [
            {'DashboardId': 'dash-001', 'Name': 'Sales Dashboard'},
            {'DashboardId': 'dash-002', 'Name': 'Profit Analysis'}
        ]
        
        generator = CSVGenerator()
        csv_content = generator.generate_dashboards_csv(dashboards)
        
        lines = [line.strip() for line in csv_content.strip().split('\n')]
        assert len(lines) == 3  
        assert lines[0] == "package_id,dashboard_id,dashboard_name,label,order,category,tags,description"
        assert ",dash-001,Sales Dashboard,,,,," in lines[1]
        assert ",dash-002,Profit Analysis,,,,," in lines[2]
        
    def test_generate_dashboards_csv_escapes_special_chars(self):
        dashboards = [
            {'DashboardId': 'dash-001', 'Name': 'Dashboard with, comma'},
            {'DashboardId': 'dash-002', 'Name': 'Dashboard with "quotes"'}
        ]
        
        generator = CSVGenerator()
        csv_content = generator.generate_dashboards_csv(dashboards)
        
        lines = csv_content.strip().split('\n')
        assert '"Dashboard with, comma"' in lines[1]
        assert '"Dashboard with ""quotes"""' in lines[2]