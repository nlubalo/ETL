"""
Running the Xetra ETL application
"""
import logging
import logging.config
import yaml

def  main():
    """
    Entry point for ETL jobs
    """
    #Pass YAML file
    config_path = "/home/nlubalo/Documents/Personal_Workspace/etl/ETL/config/xetra_repoet1_config.yml"
    config = yaml.safe_load(open(config_path))
    #configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")


if __name__ == '__main__':
    main()