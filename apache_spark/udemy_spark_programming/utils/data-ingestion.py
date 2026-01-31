# Databricks notebook source
class DataLoader():
    def __init__(self):
        self.catalog = "dev"
        self.db = "spark_db"
        self.volume_name = "datasets"
        self.owner = "LearningJournal"
        self.repo = "scholarnest_datasets"

    def cleanup_dir(self, dest_path): 
        print(f"Cleaning {dest_path}...", end='')
        dbutils.fs.rm(dest_path, recurse=True)
        dbutils.fs.mkdirs(dest_path)
        print("Done")

    def clean_ingest_data(self, source, dest):
        import requests        
        from concurrent.futures import ThreadPoolExecutor
        import collections

        dest_path = f"/Volumes/{self.catalog}/{self.db}/{self.volume_name}/{dest}"
        self.cleanup_dir(dest_path)

        api_url = f"https://api.github.com/repos/{self.owner}/{self.repo}/contents/{source}"
        files = requests.get(api_url).json()
        download_urls = [file["download_url"] for file in files]

        def download_files(download_url):
            filename = download_url.split("/")[-1]
            with requests.get(download_url, stream=True) as r:                
                with open(f"{dest_path}/{filename}", "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)               

        print(f"Downloading files...", end='')
        with ThreadPoolExecutor(max_workers=2) as executors:
            collections.deque(executors.map(download_files, download_urls))
        print("Done")