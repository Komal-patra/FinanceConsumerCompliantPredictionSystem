from FinanaceConsumerCompliant.Exception.exception import FinanceException
from FinanaceConsumerCompliant.Logger import logging
from FinanaceConsumerCompliant.utils.utils import write_yaml_file, read_yaml_file
from collections import namedtuple
import os
import sys

DataIngestionMetadataInfo =  namedtuple(
    "DataIngestionMetadataInfo",
    [
        "from_date",
        "to_date",
        "data_file_path"
    ]
)

class DataIngestionMetadata:

    def __init__(self, metadata_file_path,):
        self.metadata_file_path= metadata_file_path
    
    # --- it will return true if exist and false if doesn't exist---
    @property
    def is_metadata_file_present(self)->bool:
        return os.path.exists(self.metadata_file_path)
    

    def write_metadata_info(self, from_date:str, to_date:str, data_file_path:str):

        try:
            # Taking the Metadata info and writing it into Metadata YAML file
            metadata_info = DataIngestionMetadataInfo(
                from_date=from_date,
                to_date=to_date,
                data_file_path=data_file_path
            )
            write_yaml_file(file_path=self.metadata_file_path,data=metadata_info._asdict())

        except Exception as e:
            raise FinanceException(e,sys)

    # if Metadatafile exists then read the YAML file and the content inside it 
    def get_metadata_info(self) -> DataIngestionMetadataInfo:

        try:
            if not self.is_metadata_file_present:
                raise Exception("Metadata file is not present")
            metadata = read_yaml_file(self.metadata_file_path)
            metadata_info = DataIngestionMetadata(**(metadata))
            logging.info(metadata)
            return metadata_info
        
        except Exception as e:
            raise FinanceException(e,sys)