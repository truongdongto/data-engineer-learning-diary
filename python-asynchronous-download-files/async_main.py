import aiohttp
import asyncio
import zipfile
import logging
import os
from pathlib import Path
from typing import List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

class DataDownloader:
    def __init__(self, download_dir: Path):
        self.download_dir = download_dir
        self.download_dir.mkdir(exist_ok=True)

    async def download_file_async(self, url: str) -> Optional[Path]:
        """Download a file asynchronously using aiohttp."""
        filename = url.split('/')[-1]
        file_path = self.download_dir / filename
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        with open(file_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(8192)
                                if not chunk:
                                    break
                                f.write(chunk)
                        return file_path
                    else:
                        logger.error(f"Failed to download {url}: Status {response.status}")
                        return None
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    def extract_and_delete_zip(self, zip_path: Path) -> Optional[Path]:
        """Extract CSV from zip file and delete the zip."""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    logger.warning(f"No CSV files found in {zip_path}")
                    return None
                
                csv_filename = csv_files[0]
                zip_ref.extract(csv_filename, self.download_dir)
                csv_path = self.download_dir / csv_filename
                
            zip_path.unlink()  # Delete zip file
            return csv_path
            
        except Exception as e:
            logger.error(f"Error extracting {zip_path}: {e}")
            return None

    async def process_files_async(self, urls: List[str]):
        """Process files using async download."""
        async def process_single_file(url: str):
            zip_path = await self.download_file_async(url)
            if zip_path:
                return self.extract_and_delete_zip(zip_path)
            return None

        tasks = [process_single_file(url) for url in urls]
        return await asyncio.gather(*tasks)

async def main_async():
    script_dir = Path(__file__).parent
    downloads_dir = script_dir / "downloads"
    downloader = DataDownloader(downloads_dir)
    
    results = await downloader.process_files_async(download_uris)
    
    for url, result in zip(download_uris, results):
        if result:
            logger.info(f"Successfully processed {url} -> {result.name}")
        else:
            logger.error(f"Failed to process {url}")

if __name__ == "__main__":
    asyncio.run(main_async())