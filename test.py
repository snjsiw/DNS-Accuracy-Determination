import json

# Path to the JSON file
input_file = "C:/Users/lenovo/Desktop/test_dns/task/task_1729870816/10.5.0.17-52_54_00_aa_83_9f_2024-10-25-23-40-22_2024-10-25-23-48-39_A.json"  # Adjust the path as needed
output_file = "filtered_dns_results.json"

# List of desired domains
desired_domains = [
    "1680633.com", "1win-site-11.top", "1967x.com", "19683.top",
    "1971t.com", "130vip.com", "1wcov.top", "123down.org",
    "1wcwf.top", "123formbuilder.io"
]

# Load JSON data
with open(input_file, 'r') as file:
    data = json.load(file)

# Filter JSON data to only include the desired domains
filtered_data = {domain: data[domain] for domain in desired_domains if domain in data}

# Save the filtered data to a new JSON file
with open(output_file, 'w') as file:
    json.dump(filtered_data, file, indent=2)

print(f"Filtered data saved to {output_file}")
