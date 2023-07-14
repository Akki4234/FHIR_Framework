import json
from datetime import datetime

# Assuming the JSON object is stored in a variable called "json_data"
json_data = '''
{
    "resourceType": "OrganizationAffiliation",
    "id": "710-91a798f5-5b42-40dc-866c-6b69dbdfa06a-ProvOrganAffiliat",
    ...
}
'''

# Parse the JSON data
data = json.loads(json_data)

# Extract the id value
resource_id = data.get('id')

# Generate the CSV file name
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
csv_file_name = f"{resource_id}_{timestamp}.csv"

print("CSV file name:", csv_file_name)
