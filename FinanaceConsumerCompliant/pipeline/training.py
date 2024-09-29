from FinanaceConsumerCompliant.Exception.exception import FinanceException
from FinanaceConsumerCompliant.Logger import logging
from FinanaceConsumerCompliant.Entity.config_entity import (TrainingPipelineConfig, DataIngestionConfig)
from FinanaceConsumerCompliant.Entity.artifact_entity import (DataIngestionArtifact)
from FinanaceConsumerCompliant.components.data_ingestion import DataIngestion
import sys
import pandas as pd

class TrainingPipeline:

    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        self.training_pipeline_config: TrainingPipelineConfig = training_pipeline_config

    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            data_ingestion = DataIngestion(data_ingestion_config=data_ingestion_config)
            data_ingest_artifact = data_ingestion.initiate_data_ingestion()
            return data_ingest_artifact

        except Exception as e:
            raise FinanceException(e, sys)
        
    def start(self):
        try:
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e:
            raise FinanceException(e, sys)