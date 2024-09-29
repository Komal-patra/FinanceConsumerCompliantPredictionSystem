from collections import namedtuple
from FinanaceConsumerCompliant.Entity.config_entity import DataIngestionConfig
from FinanaceConsumerCompliant.Entity.metadata_entity import DataIngestionMetadata
from FinanaceConsumerCompliant.Entity.artifact_entity import DataIngestionArtifact
from FinanaceConsumerCompliant.Logger import logging
from FinanaceConsumerCompliant.Exception.exception import FinanceException
import os, sys
from datetime import datetime
from typing import List
import pandas as pd
import json
import requests, uuid, time
import re
from FinanaceConsumerCompliant.config.spark_manager import spark_session


DownloadUrl = namedtuple("DownloadUrl",["url","file_path","n_retry"])

class DataIngestion:

    def __init__(self, data_ingestion_config: DataIngestionConfig, n_retry: int=5,):
        
        try:
            logging.info(f"{'>>' * 20}Starting Data Ingestion Stage{'<<' * 20}")
            self.data_ingestion_config = data_ingestion_config
            self.failed_download_urls: List[DownloadUrl] = []
            self.n_retry = n_retry

        except Exception as e:
            raise FinanceException(e, sys)
        
    def get_required_interval(self):
        
        start_date = datetime.strptime(self.data_ingestion_config.from_date, "%Y-%m-%d")
        end_date = datetime.strptime(self.data_ingestion_config.to_date, "%Y-%m-%d")
        n_diff_days = (end_date - start_date).days

        freq= None
        if n_diff_days > 365:
            freq = "Y"
        elif n_diff_days > 30:
            freq = "M"
        elif n_diff_days > 7:
            freq = "W"
        logging.debug(f"{n_diff_days} Hence frequency is {freq}")

        if freq is None:
            intervals = pd.date_range(
                start=self.data_ingestion_config.from_date,
                end=self.data_ingestion_config.to_date,
                periods=2).astype('str').tolist()
            
        else:
            intervals = pd.date_range(
                start=self.data_ingestion_config.from_date,
                end=self.data_ingestion_config.to_date,
                freq=freq).astype('str').tolist()
        
        logging.debug(f" Prepared Interval : {intervals}")
        if self.data_ingestion_config.to_date not in intervals:
            intervals.append(self.data_ingestion_config.to_date)
        return intervals
    


    def download_files(self, n_day_interval_url:int = None):

        try:
            
            required_interval = self.get_required_interval()
            logging.info('started downloading the files')

            for index in range(1, len(required_interval)):
                from_date, to_date =required_interval[index-1], required_interval[index]
                logging.debug(f" Generating data download url between {from_date} and {to_date}")
                datasource_url:str = self.data_ingestion_config.datasource_url
                url = datasource_url.replace("<todate>",to_date).replace("<fromdate>", from_date)
                logging.debug(f"Generated url is {url}")
                file_name= f"{self.data_ingestion_config.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.data_ingestion_config.download_dir, file_name)
                download_url = DownloadUrl(url=url, file_path=file_path, n_retry=self.n_retry)
                self.download_data(download_url=download_url)

            logging.info(f"File downloaded URL step completed")

        except Exception as e:
            raise FinanceException(e, sys)
        

    def download_data(self, download_url: DownloadUrl):

        try:
            
            logging.info(f"Staring the download operation: {download_url}")
            download_dir = os.path.dirname(download_url.file_path)

            # creating the download directory
            os.makedirs(download_dir, exist_ok=True)

            # downloading the data 
            data = requests.get(download_url.url, params={'user-agent': f'your bot{uuid.uuid4()}'})

            try:
                logging.info(f"started writing downloaded data into json file: {download_url.file_path}")

                with open(download_url.file_path, "w") as file_obj:
                    finance_compliant_data = list(
                        map(lambda x: x["_source"],
                            filter(lambda x: "_source" in x.keys(),
                                   json.loads(data.content)))
                    )

                    json.dump(finance_compliant_data, file_obj)
                
                logging.info(f"Downloaded data has been written into the file{download_url.file_path}")

            except Exception as e:

                logging.info("failed to download the data hence retry again")

                # removing the file failed exists

                if os.path.exists(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data, download_url=download_url)


        except Exception as e:
            raise FinanceException(e, sys)
        


    def retry_download_data(self, data, download_url: DownloadUrl):

        try:
            if download_url.n_retry==0:
                self.failed_download_urls.append(download_url)
                logging.info(f"unable to download the files")
                return
            
            content = data.content.decode("utf-8")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second)  > 0:
                time.sleep(int(wait_second[0]) + 2)

            # writing response to understand why the API extracting request failed
            failed_file_path = os.path.join(self.data_ingestion_config.failed_dir,
                                            os.path.basename(download_url.file_path)
                                            )
            
            os.makedirs(self.data_ingestion_config.failed_dir, exist_ok=True)
            with open(failed_file_path, "wb") as file_obj:
                file_obj.write(data.content)

            # calling the download function again to retry
            download_url = DownloadUrl(download_url.url, file_path=download_url.file_path,
                                       n_retry=download_url.n_retry - 1)
            self.download_data(download_url=download_url)
        
        except Exception as e:
            raise FinanceException(e, sys)


    def convert_files_to_parquet(self) -> str:
        """
        Method Name: convert_files_to_parquet
        Description: Downloads files will be converted and merged into a single Parquet file.

        Output: Path to the output Parquet file.

        On Failure: Writes an exception log and then raises an exception.

        Version: 1.0
        """
        try:
            # Define the directories and output file name
            json_data_dir = self.data_ingestion_config.download_dir
            data_dir = self.data_ingestion_config.feature_store_dir
            output_file_name = self.data_ingestion_config.file_name
            
            # Create the output directory if it doesn't exist
            os.makedirs(data_dir, exist_ok=True)
            
            # Define the output file path
            file_path = os.path.join(data_dir, f"{output_file_name}.parquet")
            logging.info(f"Parquet file will be created at: {file_path}")
            
            # Check if the JSON data directory exists
            if not os.path.exists(json_data_dir):
                return file_path
            
            # Create an empty DataFrame to hold all data
            combined_df = pd.DataFrame()

            # Iterate through JSON files and convert them to Parquet
            for file_name in os.listdir(json_data_dir):
                json_file_path = os.path.join(json_data_dir, file_name)
                logging.debug(f"Converting {json_file_path} into Parquet format at {file_path}")

                # Read JSON data into a DataFrame
                df = pd.read_json(json_file_path)

                # Append the DataFrame if it contains data
                if not df.empty:
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
            
            # Write the combined DataFrame to a Parquet file
            if not combined_df.empty:
                combined_df.to_parquet(file_path, index=False)

            return file_path

        except Exception as e:
            # Log an exception message if an exception occurs during the conversion.
            logging.exception("Error during file conversion.")
            raise FinanceException(e,sys)
        
    def write_metadata(self, file_path: str) -> None:
        """
        This function help us to update metadata information 
        so that we can avoid redundant download and merging.

        """
        try:
            logging.info(f"Writing metadata info into metadata file.")
            metadata_info = DataIngestionMetadata(metadata_file_path=self.data_ingestion_config.metadata_file_path)

            metadata_info.write_metadata_info(from_date=self.data_ingestion_config.from_date,
                                              to_date=self.data_ingestion_config.to_date,
                                              data_file_path=file_path
                                              )
            logging.info(f"Metadata has been written.")
        except Exception as e:
            raise FinanceException(e, sys)
    
    def initiate_data_ingestion(self) -> DataIngestionArtifact:
        try:
            logging.info(f"Started downloading json file")
            if self.data_ingestion_config.from_date != self.data_ingestion_config.to_date:
                self.download_files()

            if os.path.exists(self.data_ingestion_config.download_dir):
                logging.info(f"Converting and combining downloaded json into parquet file")
                file_path = self.convert_files_to_parquet()
                self.write_metadata(file_path=file_path)

            feature_store_file_path = os.path.join(self.data_ingestion_config.feature_store_dir,
                                                   self.data_ingestion_config.file_name)
            artifact = DataIngestionArtifact(
                feature_store_file_path=feature_store_file_path,
                download_dir=self.data_ingestion_config.download_dir,
                metadata_file_path=self.data_ingestion_config.metadata_file_path,

            )

            logging.info(f"Data ingestion artifact: {artifact}")
            return artifact
        
        except Exception as e:
            raise FinanceException(e, sys)



