#!/usr/bin/env python

from datetime import datetime
import os
import requests
import zipfile

import pandas as pd
from prefect import flow, task


@task(timeout_seconds=60, retries=3, retry_delay_seconds=30)
def check_update(url, last_update_file) -> bool:
    if not os.path.exists(last_update_file):
        return True

    with open(last_update_file) as f:
        last_update = f.read().strip()
        last_saved_dt = datetime.strptime(last_update, "%a, %d %b %Y %H:%M:%S %Z")

    # Read HEAD's last-modified
    response = requests.head(url, allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"HEAD request failed with status {response.status_code}")
    else:
        last_modified = response.headers.get("last-modified")
        if not last_modified:
            print("Server did not provide last-modified. Downloading anyway.")
            return True
        last_modified_dt = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")

    if last_modified_dt <= last_saved_dt:
        print("No updates needed")
        return False
    else:
        return True


@task(timeout_seconds=60, retries=3, retry_delay_seconds=30)
def download_zip(url, last_update_file) -> str:
    zip_path = "/tmp/generacion_erenovables.zip"
    response = requests.get(url, stream=True)

    if response.status_code != 200: 
        raise Exception(f"Failed to download file: status {response.status_code}")

    with open(last_update_file, "w") as f:
        f.write(response.headers.get('last-modified'))

    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded {zip_path} successfully.")

    return zip_path


@task
def extract_xlsx(zip_path) -> str:
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall("/tmp")
        xlsx_filename = zip_ref.namelist()[0] # is only one file

    xlsx_path = os.path.join("/tmp", xlsx_filename)

    print(f"Extracted file {xlsx_path}.")
    return xlsx_path


@task(log_prints=True)
def load_xlsx(xlsx_path, csv_path) -> None:
    df = pd.read_excel(
        xlsx_path,
        sheet_name="Base de Datos",
        skiprows=1,   # skip the first row
        header=0      # the next row becomes the header
    )

    df.to_csv(csv_file, index=False)
    return 


@flow
def etl_erenovables(url, last_update_file, csv_file):
    new_update = check_update(url, last_update_file)

    if new_update:
        zip_path = download_zip(url, last_update_file)
        xlsx_path = extract_xlsx(zip_path)
        load_xlsx(xlsx_path, csv_file)


if __name__ == "__main__":
    url = "https://cammesaweb.cammesa.com/erenovables/?wpdmdl=37500"
    erenovables_dir = os.path.expanduser("~/energias_renovables")
    last_update_file = os.path.join(erenovables_dir, "last_update.txt")
    csv_file = os.path.join(erenovables_dir, "erenovables.csv")

    os.makedirs(erenovables_dir, exist_ok=True)
    etl_erenovables(url, last_update_file, csv_file)
