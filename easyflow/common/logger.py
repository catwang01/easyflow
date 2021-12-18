# logger.py
import logging
import os
import yaml
import logging.config
from importlib import resources

def setupLogger(loggerName, configPath=None, default_level=logging.INFO):
    if configPath and os.path.exists(configPath):
        with open(configPath, 'r', encoding='utf-8') as f:
            config = yaml.load(f)
    else:
        with resources.open_text('easyflow.common', 'config.yaml') as f:
            config = yaml.load(f)
    logging.config.dictConfig(config)
    logging.basicConfig(level=default_level)
    return logging.getLogger(loggerName)