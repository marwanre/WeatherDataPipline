import requests
from datetime import datetime, timedelta
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import tempfile
from google.cloud import bigquery

### Collecting weather data from weatherapi.com and storing it in Google Sheets and BigQuery
# Set up the API key and locations
API_KEY = "367ad2948c5f4007a1a194056252703"
locations =[
    "Cairo", "Giza", "Alexandria", "Port Said", "Suez", "Ismailia", "Dakahlia", "Damietta", "Qalyubia", 
    "Kafr El-Sheikh", "Faiyum", "Beni Suef", "Assiut", "Sohag", "Qena", "Luxor", "Aswan", 
    "Marsa Matruh", "Monufia"]


# Get the last 3 days from today
END_DATE = (datetime.today() - timedelta(days=0)).strftime('%Y-%m-%d')  # today
START_DATE = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')  # 3 days ago
response_Location = []
for location in locations:
    url = f"https://api.weatherapi.com/v1/history.json?key={API_KEY}&q={location}&dt={START_DATE}&end_dt={END_DATE}"
    response_Location.append(requests.get(url).json())
    
    
data_list = []

for i in response_Location:
    if "forecast" not in i:  # Check if "forecast" exists
        print(f" Skipping: No forecast data for {i.get('location', {}).get('name', 'Unknown Location')}")
        continue  # Skip this location

    for j in i["forecast"]["forecastday"]:  # Loop through each day's data separately
        data = {                # Create a NEW dictionary for each day
            "date": j["date"], 
            "name": i["location"]["name"],
            "region": i["location"]["region"],
            "country": i["location"]["country"],
            "lat": i["location"]["lat"],
            "lon": i["location"]["lon"],
            "tz_id": i["location"]["tz_id"],
            "maxtemp_c": j["day"]["maxtemp_c"],
            "mintemp_c": j["day"]["mintemp_c"],
            "avgtemp_c": j["day"]["avgtemp_c"],
            "maxwind_kph": j["day"]["maxwind_kph"],
            "totalprecip_mm": j["day"]["totalprecip_mm"],
            "avgvis_km": j["day"]["avgvis_km"],
            "avghumidity": j["day"]["avghumidity"],
            "daily_will_it_rain": j["day"]["daily_will_it_rain"],
            "daily_chance_of_rain": j["day"]["daily_chance_of_rain"],
            "daily_will_it_snow": j["day"]["daily_will_it_snow"],
            "daily_chance_of_snow": j["day"]["daily_chance_of_snow"],
            "condition_text": j["day"]["condition"]["text"],
            "condition_icon": j["day"]["condition"]["icon"],
            "sunrise": j["astro"]["sunrise"],
            "sunset": j["astro"]["sunset"],
            "moonrise": j["astro"]["moonrise"],
            "moonset": j["astro"]["moonset"],
            "moon_phase": j["astro"]["moon_phase"],
            "moon_illumination": j["astro"]["moon_illumination"]
        }
        data_list.append(data)  # Append the NEW dictionary for each day

# Convert list to Pandas DataFrame
df = pd.DataFrame(data_list)

###############################################################################################################################
###############################################################################################################################
# Uploading data to Google Sheets

# Setup Google Sheets API authentication
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("key2.json", scope)
client = gspread.authorize(creds)

# Open the Google Sheet
spreadsheet = client.open_by_key("12mgjUex8yb9TFB9qllQyLPa6f_Zsv5Y2IqfOwDbQxBo") # with sheet ID
worksheet = spreadsheet.sheet1



# Read existing data into a DataFrame
existing_data = pd.DataFrame(worksheet.get_all_records()).astype(str)
# Get the new data (example DataFrame)
new_data = df.astype(str)  # Ensure data is in string format

if existing_data.shape[0] == 0:
    # If the sheet is empty, create an empty DataFrame with headers
    existing_data = new_data.copy() # Create an empty DataFrame with headers


# Identify the dates
old_dates = set(existing_data['date'])  # Existing sheet dates
new_dates = set(new_data['date'])  # New fetched dates

# Find the past date that should remain (the oldest one)
past_date_to_keep = list(old_dates - new_dates) if old_dates - new_dates else None

# Combine past data + new data
if past_date_to_keep:
    past_data = existing_data[existing_data['date'].isin(past_date_to_keep)]
    final_data = pd.concat([past_data, new_data], ignore_index=True)
else:
    final_data = new_data  # If there's no past data to keep, just use the new data

final_data = final_data.sort_values(by="date", ascending=True)

# Upload to Google Sheets
worksheet.clear()  # Clear only after preparing the data to avoid data loss
worksheet.update([final_data.columns.tolist()] + final_data.values.tolist())

print("Sheet updated with new data for recent dates!")

###############################################################################################################################
###############################################################################################################################
# Uploading data to BigQuery

# Open Google Sheet by name
spreadsheet = client.open("weatherdata")  #  with sheet name
worksheet = spreadsheet.sheet1  # Access first sheet

# Get all rows from the Google Sheet (excluding the header row)
rows = pd.DataFrame(worksheet.get_all_records()).astype(str)

# Ensure 'date' column is in datetime format
rows['date'] = pd.to_datetime(rows['date'], errors='coerce')  # Add error handling for invalid dates

# Get the most recent date in the DataFrame
latest_date = rows['date'].max()

# Calculate the date 30 days before that
start_date = latest_date - timedelta(days=30)

# Filter the DataFrame to only include rows in that 30-day window
rows = rows[rows['date'] >= start_date]
rows = rows.to_dict(orient='records')  # Convert to list of dictionaries

# Path to your service account JSON file
service_account_path = "key2.json"

# Initialize BigQuery client with the service account key
bq_client = bigquery.Client.from_service_account_json(service_account_path)

# Set project and dataset details
project_id = "weatherdataautomation-455720"  # Replace with your GCP project ID
dataset_id = "WeatherData"  # Replace with your dataset name
table_id = "WeatherData"  # Replace with your table name

# Define the dataset reference
dataset_ref = bq_client.dataset(dataset_id)

# Check if the dataset exists, and if so, delete it
try:
    bq_client.get_dataset(dataset_ref)  # Check if the dataset exists
    print(f"Dataset {dataset_id} exists. Deleting it.")
    bq_client.delete_dataset(dataset_ref, delete_contents=True)  # Delete dataset and its contents
    print(f"Dataset {dataset_id} deleted.")
except Exception as e:
    print(f"Dataset {dataset_id} does not exist. Proceeding to create a new one.")

# Create the dataset again
dataset = bigquery.Dataset(dataset_ref)
dataset.location = "US"  # Change the region if needed
bq_client.create_dataset(dataset)  # Create dataset
print(f"Dataset {dataset_id} created.")

# Create the table again with the same schema
table_ref = dataset_ref.table(table_id)
try:
    bq_client.get_table(table_ref)  # Check if the table exists
    print(f"Table {table_id} exists. Deleting it.")
    bq_client.delete_table(table_ref)  # Delete the existing table
    print(f"Table {table_id} deleted.")
except Exception as e:
    print(f"Table {table_id} does not exist. Proceeding to create a new table.")

# Create the table with the specified schema
schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("lat", "FLOAT"),
    bigquery.SchemaField("lon", "FLOAT"),
    bigquery.SchemaField("tz_id", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("maxtemp_c", "FLOAT"),
    bigquery.SchemaField("mintemp_c", "FLOAT"),
    bigquery.SchemaField("avgtemp_c", "FLOAT"),
    bigquery.SchemaField("maxwind_kph", "FLOAT"),
    bigquery.SchemaField("totalprecip_mm", "FLOAT"),
    bigquery.SchemaField("avgvis_km", "FLOAT"),
    bigquery.SchemaField("avghumidity", "FLOAT"),
    bigquery.SchemaField("daily_will_it_rain", "STRING"),
    bigquery.SchemaField("daily_chance_of_rain", "STRING"),
    bigquery.SchemaField("daily_will_it_snow", "STRING"),
    bigquery.SchemaField("daily_chance_of_snow", "STRING"),
    bigquery.SchemaField("condition_text", "STRING"),
    bigquery.SchemaField("condition_icon", "STRING"),
    bigquery.SchemaField("sunrise", "STRING"),
    bigquery.SchemaField("sunset", "STRING"),
    bigquery.SchemaField("moonrise", "STRING"),
    bigquery.SchemaField("moonset", "STRING"),
    bigquery.SchemaField("moon_phase", "STRING"),
    bigquery.SchemaField("moon_illumination", "FLOAT"),
]

table = bigquery.Table(table_ref, schema=schema)
bq_client.create_table(table)  # Create the new table
print(f"Table {table_id} created with the specified schema.")

# Prepare the data for loading into BigQuery
rows_to_insert = []
for row in rows:
    rows_to_insert.append({
        "name": row["name"],
        "region": row["region"],
        "country": row["country"],
        "lat": row["lat"],
        "lon": row["lon"],
        "tz_id": row["tz_id"],
        "date": row["date"].strftime('%Y-%m-%d'),  # Ensure date is in string format
        "maxtemp_c": row["maxtemp_c"],
        "mintemp_c": row["mintemp_c"],
        "avgtemp_c": row["avgtemp_c"],
        "maxwind_kph": row["maxwind_kph"],
        "totalprecip_mm": row["totalprecip_mm"],
        "avgvis_km": row["avgvis_km"],
        "avghumidity": row["avghumidity"],
        "daily_will_it_rain": row["daily_will_it_rain"],
        "daily_chance_of_rain": row["daily_chance_of_rain"],
        "daily_will_it_snow": row["daily_will_it_snow"],
        "daily_chance_of_snow": row["daily_chance_of_snow"],
        "condition_text": row["condition_text"],
        "condition_icon": row["condition_icon"],
        "sunrise": row["sunrise"],
        "sunset": row["sunset"],
        "moonrise": row["moonrise"],
        "moonset": row["moonset"],
        "moon_phase": row["moon_phase"],
        "moon_illumination": row["moon_illumination"],
    })

###########################################################################################################################################################
#####################################################################################################################
# Create a temporary file to write the data
temp_file_path = None  # Initialize temp_file_path to ensure it's defined
if rows_to_insert:
    try:
        with tempfile.NamedTemporaryFile(delete=False, mode='w+', newline='') as temp_file:
            for row in rows_to_insert:
                json_line = json.dumps(row)  # Convert the row to a JSON string
                temp_file.write(json_line + "\n")  # Write each JSON object on a new line
            temp_file_path = temp_file.name  # Store the path of the temp file

        # Load the temporary JSON file into BigQuery
        with open(temp_file_path, 'rb') as f:
            load_job = bq_client.load_table_from_file(
                f,
                table_ref,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                )
            )
        load_job.result()  # Wait for the job to complete
        print(f"Data uploaded to BigQuery table: {table_ref.path}")

    except Exception as e:
        print(f"Error: {e}")  # Print detailed error message

    finally:
        # Delete the temporary file if it exists
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
            print(f"Temporary file deleted: {temp_file_path}")
