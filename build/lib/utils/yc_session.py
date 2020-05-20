import os
import time
from urllib.parse import urlparse
import boto3
import botocore

AWS_KEYS_REFRESH_INTERVAL_IN_SEC = 3600


class YcSession:
    def __init__(self):
        self.aws_keys_last_refresh_time = 0
        self.keys = {}

    def get_aws_keypair(self):
        time_diff_in_sec = int(time.time()) - int(self.aws_keys_last_refresh_time)
        if time_diff_in_sec > AWS_KEYS_REFRESH_INTERVAL_IN_SEC:
            self.keys = self.retrieve_temp_cred_if_present()
        return self.keys

    @staticmethod
    def retrieve_temp_cred_if_present():
        import boto3
        keys = {}
        if os.getenv("AWS_ROLE_ARN") and os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"):
            sts_connection = boto3.client('sts')
            role_arn = os.getenv("AWS_ROLE_ARN")

            with open(os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE"), 'r') as content_file:
                web_identity_token = content_file.read()
            print('role_arn: {}'.format(role_arn))
            print('web_identity_token: {}'.format(web_identity_token))
            assume_role_object = sts_connection.assume_role_with_web_identity(
                   RoleArn=role_arn,
                   RoleSessionName="mlflow_s3_session",
                   WebIdentityToken=web_identity_token
            )
            keys['aws_access_key_id']=assume_role_object['Credentials']['AccessKeyId']
            keys['aws_secret_access_key']=assume_role_object['Credentials']['SecretAccessKey']
            keys['aws_session_token']=assume_role_object['Credentials']['SessionToken']
        return keys

    def get_boto_session(self):
        keys_map = self.get_aws_keypair()

        return boto3.Session(
            aws_access_key_id=keys_map["aws_access_key_id"],
            aws_secret_access_key=keys_map["aws_secret_access_key"],
            aws_session_token=keys_map["aws_session_token"]
        )

    @staticmethod
    def get_bucket_and_key_from_s3_path(path):
        url_obj = urlparse(path)
        if url_obj.scheme in ["s3", "s3a"]:
            bucket = url_obj.netloc
            key = url_obj.path
            return bucket, key
        else:
            raise botocore.exceptions.InvalidS3AddressingStyleError("Invalid S3 scheme")

    def write_file_to_s3(self, src_path, tar_path):
        bucket, key = self.get_bucket_and_key_from_s3_path(tar_path)
        session = self.get_boto_session()
        s3 = session.resource("s3")
        s3.meta.client.upload_file(src_path, bucket, key[1:])

    def get_data_from_s3(self, s3_file_path):
        bucket, key = self.get_bucket_and_key_from_s3_path(s3_file_path)
        keys_map = self.get_aws_keypair()
        client = boto3.client("s3", aws_access_key_id=keys_map["aws_access_key_id"],
                              aws_secret_access_key=keys_map["aws_secret_access_key"],
                              aws_session_token=keys_map["aws_session_token"]
                              )
        obj = client.get_object(Bucket=bucket, Key=key[1:])["Body"].read()
        return obj

    def write_dataframe_on_s3(self, dataframe, tar_file_path, data_format="csv"):
        from io import StringIO

        filename = os.path.basename(tar_file_path)
        tar_dir = tar_file_path.split(filename)[0]
        buffer = StringIO()
        if data_format == "csv":
            dataframe.to_csv(buffer, sep="|", index=False)
        elif data_format == "json":
            dataframe.to_json(buffer, index=False)

        session = self.get_boto_session()
        s3_resource = session.resource("s3")
        s3_resource.Object(tar_dir, filename).put(Body=buffer.getvalue())

    def read_s3_data_into_dataframe(self, pd, src_path, data_format="csv"):
        obj = self.get_data_from_s3(src_path)
        if data_format == "csv":
            return pd.read_csv(obj)
        elif data_format == "json":
            return pd.read_json(obj)
        elif data_format == "parquet":
            return pd.read_parquet(obj)
