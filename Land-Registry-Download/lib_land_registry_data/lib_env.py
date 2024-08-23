
import os

from lib_land_registry_data.logging import get_logger

logger = get_logger()


class EnvironmentVariables():
    
    def __init__(self) -> None:
        logger.info(f'read environment variables')
        
        kafka_bootstrap_servers = os.environ['KAFKA_BOOTSTRAP_SERVERS']
        logger.info(f'env: KAFKA_BOOTSTRAP_SERVERS={kafka_bootstrap_servers}')
        
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

        postgres_host = os.environ['POSTGRES_HOST']
        postgres_user = os.environ['POSTGRES_USER']
        postgres_password = os.environ['POSTGRES_PASSWORD']
        postgres_database = os.environ['POSTGRES_DATABASE']
        logger.info(f'env: POSTGRES_HOST={postgres_host}')
        logger.info(f'env: POSTGRES_USER={postgres_user}')
        logger.info(f'env: POSTGRES_DATABASE={postgres_database}')
        
        self.postgres_host = postgres_host
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_database = postgres_database
        
        aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        
    def get_kafka_bootstrap_servers(self) -> str:
        return self.kafka_bootstrap_servers
    
    def get_postgres_connection_string(self) -> str:
        postgres_user = self.postgres_user
        postgres_password = self.postgres_password
        postgres_host = self.postgres_host
        postgres_database = self.postgres_database
        postgres_connection_string = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}/{postgres_database}'
        return postgres_connection_string
    
    def get_aws_access_key_id(self) -> str:
        return self.aws_access_key_id
    
    def get_aws_secret_access_key(self) -> str:
        return self.aws_secret_access_key
    