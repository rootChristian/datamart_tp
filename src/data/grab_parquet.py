"""
***********************************************************************
************** Author:   Christian KEMGANG NGUESSOP *******************
************** Project:   datamart                  *******************
************** Version:  1.0.0                      *******************
***********************************************************************
"""

import sys
from data_function import (
    download_all_files,
    download_single_file,
    upload_to_minio,
    write_data_minio,
)


def main():
    grab_data()


def grab_data() -> None:
    """
    Download all files months in local
    """
    download_all_files()

    """
        Download a single last month file in local
    """
    download_single_file()

    """
        Upload all files to Minio
    """
    write_data_minio()

    """
        Upload a single file to Minio
    """
    file_name = "yellow_tripdata_2024-08.parquet"
    file_path = "../../data/raw"
    upload_to_minio(file_name, file_path)


if __name__ == "__main__":
    sys.exit(main())
