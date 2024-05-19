from minio import Minio
import urllib.request
import pandas as pd
import sys
import os

def main():
    grab_data()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    
    for i in range(1, 13):
        # Add 0 before the number if it is less than 10
        if i < 10:
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-0" + str(i) + ".parquet"
        else:
            url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-" + str(i) + ".parquet"
        print("Downloading file: " + url)
        urllib.request.urlretrieve(url, "../../data/raw/yellow_tripdata_2023-" + str(i) + ".parquet")
    write_data_minio()

def grab_data_last_month() -> None:
    time = pd.Timestamp.now()
    #Téléchargement du fichier du mois précédent
    print("Downloading file: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_" + str(time.year) + "-" + str(time.month - 1) + ".parquet")
    urllib.request.urlretrieve("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_" + str(time.year) + "-" + str(time.month - 1) + ".parquet", "../../data/raw/yellow_tripdata_" + str(time.year) + "-" + str(time.month - 1) + ".parquet")
    print("Fichier téléchargé")
    write_data_minio()


def write_data_minio():
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "taxi"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

    #Envoi des fichiers dans le bucket
    for i in range(1, 13):
        file = "../../data/raw/yellow_tripdata_2023-" + str(i) + ".parquet"
        client.fput_object(bucket, "yellow_tripdata_2023-" + str(i) + ".parquet", file)
        print("Fichier " + file + " envoyé dans le bucket " + bucket)


if __name__ == '__main__':
    sys.exit(main())
