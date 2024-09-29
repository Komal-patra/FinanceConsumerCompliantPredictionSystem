from dataclasses import dataclass
import os
from FinanaceConsumerCompliant.Logger import TIMESTAMP
from FinanaceConsumerCompliant.Entity.metadata_entity import DataIngestionMetadata
from FinanaceConsumerCompliant.Exception.exception import FinanceException
from datetime import datetime
import sys

# Data Ingestion Constant
DATA_INGESTION_DIR="data_ingestion"
DATA_INGESTION_DOWNLOADED_DIR="downloaded_files"
DATA_INGESTION_FILE_NAME="finance_compliant"
DATA_INGESTION_FEATURE_STORE_DIR="feature_store"
DATA_INGESTION_FAILED_DIR="failed_downloaded_files"
DATA_INGESTION_METADATA_FILE_NAME="meta_info.yaml"
DATA_INGESTION_MIN_START_DATE="2023-01-01"
DATA_INGESTION_DATA_SOURCE_URL=f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?field=all&format=json&no_aggs=false&no_highlight=false&date_received_max=<todate>&date_received_min=<fromdate>"

# Training PipelineConfig
@dataclass
class TrainingPipelineConfig:
    pipeline_name:str="artifact"
    artifact_dir:str = os.path.join(pipeline_name,TIMESTAMP)

# Data IngestionConfig
class DataIngestionConfig:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig,
                 from_date=DATA_INGESTION_MIN_START_DATE,
                 to_date=None):
        
        try:
            self.from_date=from_date
            min_start_date=datetime.strptime(DATA_INGESTION_MIN_START_DATE,"%Y-%m-%d")
            from_data_obj = datetime.strptime(from_date,"%Y-%m-%d")

            if from_data_obj < min_start_date:
                self.from_date = DATA_INGESTION_MIN_START_DATE
            
            if to_date is None:
                self.to_date = datetime.now().strftime("%Y-%m-%d")

            data_ingestion_master_dir =os.path.join(os.path.dirname(training_pipeline_config.artifact_dir),DATA_INGESTION_DIR)
            self.data_ingestion_dir = os.path.join(data_ingestion_master_dir,TIMESTAMP)
            self.metadata_file_path = os.path.join(data_ingestion_master_dir,DATA_INGESTION_METADATA_FILE_NAME)

            data_ingestion_metadata = DataIngestionMetadata(metadata_file_path=self.metadata_file_path)
            if data_ingestion_metadata.is_metadata_file_present:
                metadata_info = data_ingestion_metadata.get_metadata_info()
                self.from_date = metadata_info.to_date

            self.download_dir = os.path.join(self.data_ingestion_dir,DATA_INGESTION_DOWNLOADED_DIR)
            self.failed_dir= os.path.join(self.data_ingestion_dir, DATA_INGESTION_FAILED_DIR)
            self.file_name = DATA_INGESTION_FILE_NAME
            self.feature_store_dir = os.path.join(self.data_ingestion_dir,DATA_INGESTION_FEATURE_STORE_DIR)
            self.datasource_url = DATA_INGESTION_DATA_SOURCE_URL

        except Exception as e:
            raise FinanceException(e,sys)
        
    