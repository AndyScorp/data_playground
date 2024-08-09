import requests
import pandas as pd
from io import StringIO

# Step 1: Retrieve the list of tr IDs
api_key_hash = "032eaea3"
url = 'https://api.xy.com/list'
headers = {'Content-Type': 'application/json'}
data = {'hash': api_key_hash}

response = requests.post(url, headers=headers, json=data)
tracker_list = response.json()['list']
tracker_ids = [tracker['id'] for tracker in tracker_list]

# Step 2: Retrieve raw data for each tr ID
raw_data_url = 'https://api.xy.com/read'
raw_data_headers = {
    'accept': 'text/csv',
    'Content-Type': 'application/json'
}

# Define the time range and columns for the raw data request
from_time = "2024-07-01T00:00:00Z"
to_time = "2024-07-08T23:59:59Z"
columns = [
    "lat",
    "lng",
    "speed"
]

# List to store DataFrames
dataframes = []

for tracker_id in tracker_ids:
    raw_data_payload = {
        "hash": api_key_hash,
        "tracker_id": tracker_id,
        "from": from_time,
        "to": to_time,
        "columns": columns
    }

    raw_data_response = requests.post(raw_data_url, headers=raw_data_headers, json=raw_data_payload)
    csv_data = raw_data_response.text

    # Read CSV data into a DataFrame
    df = pd.read_csv(StringIO(csv_data))

    # Add the 'id' column as the first column
    df.insert(0, 'id', tracker_id)

    # Ensure all columns are present, filling missing columns with NULL
    for col in columns:
        if col not in df.columns:
            df[col] = 'NULL'

    dataframes.append(df)

# Step 3 Concatenate all DataFrames
final_df = pd.concat(dataframes, ignore_index=True)

# Save the final DataFrame to a CSV file
final_df.to_csv('all_raw_data.csv', index=False)

print("Data concatenation complete. Saved to 'all_raw_data.csv'.")
