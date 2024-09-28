from collections import namedtuple
from FinanaceConsumerCompliant.Entity.config_entity import DataIngestionConfig
from FinanaceConsumerCompliant.Entity.metadata_entity import DataIngestionMetadata
from FinanaceConsumerCompliant.Entity.artifact_entity import DataIngestionArtifact
from FinanaceConsumerCompliant.Logger import logging
from FinanaceConsumerCompliant.Exception.exception import FinanceException
import os, sys, datetime
from typing import List
import pandas as pd
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
                end_date=self.data_ingestion_config.to_date,
                periods=2).astype('str').tolist()
            
        else:
            intervals = pd.date_range(
                start=self.data_ingestion_config.from_date,
                end_date=self.data_ingestion_config.to_date,
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

            logging.info(f"File downloade step completed")

        except Exception as e:
            raise FinanceException(e, sys)
        

    def download_data(self, download_url: DownloadUrl):

        try:
            pass
        
        except Exception as e:
            raise FinanceException(e, sys)
        
