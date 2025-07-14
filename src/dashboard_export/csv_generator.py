import csv
from io import StringIO
from typing import List, Dict


class CSVGenerator:
    def generate_packages_csv(self, dashboards: List[Dict]) -> str:
        output = StringIO()
        writer = csv.writer(output)
        
        headers = ['package_id', 'bizuser_code', 'label', 'required', 'delete']
        writer.writerow(headers)
        
        for dashboard in dashboards:
            writer.writerow(['', '', '', '', ''])
            
        return output.getvalue()
        
    def generate_dashboards_csv(self, dashboards: List[Dict]) -> str:
        output = StringIO()
        writer = csv.writer(output)
        
        headers = ['package_id', 'dashboard_id', 'dashboard_name', 'label', 
                  'order', 'category', 'tags', 'description']
        writer.writerow(headers)
        
        for dashboard in dashboards:
            row = [
                '',  
                dashboard['DashboardId'],
                dashboard['Name'],
                '',  
                '',  
                '',  
                '',  
                ''   
            ]
            writer.writerow(row)
            
        return output.getvalue()