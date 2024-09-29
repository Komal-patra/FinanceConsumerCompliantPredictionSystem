from FinanaceConsumerCompliant.Exception.exception import FinanceException
import yaml
import sys
import os

def write_yaml_file(file_path:str, data:dict=None):
    """
    Write data to a yaml file.
    file_path: str
    data:dict
    """
    try:
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)
    
def read_yaml_file(file_path:str, data:dict=None):
    """
    Read data From a yaml file and returns the content as a dictionary.
    file_path: str
    """
    try:
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path, 'w') as yaml_file:
            yaml.dump(data, yaml_file)
    except Exception as e:
        raise FinanceException(e,sys)