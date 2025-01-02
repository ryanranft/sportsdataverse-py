import os
import boto3
import logging
from botocore.exceptions import ClientError
from dotenv import load_dotenv


class AWSServiceConfigurator:
    def __init__(self):
        load_dotenv()
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.aws_region = os.getenv('AWS_REGION')
        self.firehose_stream_name = os.getenv('FIREHOSE_STREAM_NAME')
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME')
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def create_session(self):
        try:
            self.session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            self.logger.info("AWS session created successfully")
        except Exception as e:
            self.logger.error(f"Failed to create AWS session: {str(e)}")
            raise

    def setup_s3(self):
        try:
            s3 = self.session.client('s3')
            if not any(bucket['Name'] == self.s3_bucket_name
                      for bucket in s3.list_buckets()['Buckets']):
                s3.create_bucket(
                    Bucket=self.s3_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.aws_region
                    } if self.aws_region != 'us-east-1' else {}
                )
                self.logger.info(f"Created S3 bucket: {self.s3_bucket_name}")
            else:
                self.logger.info(f"S3 bucket already exists: {self.s3_bucket_name}")
            return True
        except ClientError as e:
            self.logger.error(f"S3 setup error: {str(e)}")
            return False

    def setup_firehose(self):
        try:
            firehose = self.session.client('firehose')

            delivery_stream_config = {
                'DeliveryStreamName': self.firehose_stream_name,
                'DeliveryStreamType': 'DirectPut',
                'S3DestinationConfiguration': {
                    'RoleARN': os.getenv('FIREHOSE_ROLE_ARN'),
                    'BucketARN': f'arn:aws:s3:::{self.s3_bucket_name}',
                    'Prefix': 'sports-data/',  # Simplified prefix
                    'ErrorOutputPrefix': 'errors/!{firehose:error-output-type}/',
                    'BufferingHints': {
                        'SizeInMBs': 5,
                        'IntervalInSeconds': 300
                    },
                    'DynamicPartitioningConfiguration': {
                        'Enabled': True
                    }
                }
            }

            try:
                firehose.describe_delivery_stream(DeliveryStreamName=self.firehose_stream_name)
                self.logger.info(f"Firehose delivery stream already exists: {self.firehose_stream_name}")
            except firehose.exceptions.ResourceNotFoundException:
                firehose.create_delivery_stream(**delivery_stream_config)
                self.logger.info(f"Created Firehose delivery stream: {self.firehose_stream_name}")

            return True
        except ClientError as e:
            self.logger.error(f"Firehose setup error: {str(e)}")
            return False

    def setup_athena(self):
        try:
            athena = self.session.client('athena')
            database_name = 'sportsdataverse'

            # Create Athena database
            query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
            athena.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': f's3://{self.s3_bucket_name.lower()}/athena-results/'
                }
            )
            self.logger.info(f"Created/Verified Athena database: {database_name}")
            return True
        except ClientError as e:
            self.logger.error(f"Athena setup error: {str(e)}")
            return False

    def setup_cloudwatch(self):
        try:
            cloudwatch = self.session.client('logs')
            log_group_name = '/aws/sports-data'  # Default log group name

            try:
                cloudwatch.create_log_group(logGroupName=log_group_name)
                self.logger.info(f"Created CloudWatch log group: {log_group_name}")
            except cloudwatch.exceptions.ResourceAlreadyExistsException:
                self.logger.info(f"CloudWatch log group already exists: {log_group_name}")

            return True
        except ClientError as e:
            self.logger.error(f"CloudWatch setup error: {str(e)}")
            return False


def main():
    # Initialize configurator
    configurator = AWSServiceConfigurator()
    configurator.create_session()

    # Setup variables
    bucket_name = os.getenv('BUCKET_NAME')
    delivery_stream_name = os.getenv('DELIVERY_STREAM_NAME')
    database_name = os.getenv('DATABASE_NAME')
    log_group_name = os.getenv('LOG_GROUP_NAME')

    # Run setup
    services_setup = {
        'S3': configurator.setup_s3(),
        'Firehose': configurator.setup_firehose(),
        'Athena': configurator.setup_athena(),
        'CloudWatch': configurator.setup_cloudwatch()
    }

    # Print setup results
    print("\nSetup Results:")
    for service, success in services_setup.items():
        print(f"{service}: {'✓ Success' if success else '✗ Failed'}")


if __name__ == "__main__":
    main()
