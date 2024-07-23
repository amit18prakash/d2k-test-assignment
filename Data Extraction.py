import os
import requests
from tqdm import tqdm

def download_file(url, dest_folder, filename):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    
    file_path = os.path.join(dest_folder, filename)
    
    # Check if file already exists
    if os.path.exists(file_path):
        print(f"{filename} already exists. Skipping download.")
        return file_path
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(file_path, "wb") as file:
        for chunk in tqdm(response.iter_content(chunk_size=8192), desc=filename):
            file.write(chunk)
    
    print(f"Downloaded {filename}")
    return file_path

def download_taxi_data(year):
    base_url = "https://data.cityofnewyork.us/api/views/kxp8-n2sj/rows.csv?accessType=DOWNLOAD"
    
    for month in range(1, 13):
        file_url = f"{base_url}&year={year}&month={month:02d}"
        filename = f"yellow_tripdata_{year}-{month:02d}.csv"
        download_file(file_url, f"data/{year}", filename)

if __name__ == "__main__":
    download_taxi_data(2019)
