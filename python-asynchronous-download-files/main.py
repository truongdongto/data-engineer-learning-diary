#import necessary modules and packages
import zipfile
from pathlib import Path
import requests

#uris
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

#download processing
def download_file(url: str, download_path: Path) -> bool:
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(download_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
    except requests.exceptions.RequestException as e:
        print(e)
        return False
    
#processing zip file
def extract_delete_file(zip_path: Path, extract_dir: Path) -> Path | None:
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            csv_files =  [f for f in zip_ref.namelist() if f.lower().endswith(".csv")]
            if not csv_files:
                print(f"No csv file exists in {zip_path}")
                return None
            
            csv_filename = csv_files[0]
            csv_path = zip_ref.extract(csv_filename, extract_dir)

        zip_path.unlink()
        return csv_path
    except zipfile.BadZipFile as e:
        print(e)
    except Exception as e:
        print(e)
    return None

def main():
    script_dir = Path(__file__).parent
    downloads_dir = (script_dir / "downloads")
    downloads_dir.mkdir(exist_ok=True)

    for uri in download_uris:
        filename = uri.split('/')[-1]
        zip_path = downloads_dir / filename
        print(f"Downloading {filename} ...")
        if download_file(uri, zip_path):
            print(f"Downloaded {filename}")
            if extract_delete_file(zip_path, downloads_dir):
                print(f"Extracted {filename} successfully")
            else:
                print(f"Failed to extract {filename}")
        else:
            print(f"Failed to download {filename}")

if __name__ == "__main__":
    main()