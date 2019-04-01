import boto3
from botocore.exceptions import ClientError

from .parameter_store import ParameterStore


class AwsParameterStore(ParameterStore):
    """
    Retrieve or store values from/into the AWS Parameter Store.
    """

    def __init__(self, region_name: str = "eu-west-1"):
        self.ssm = boto3.client("ssm", region_name=region_name, verify=None)

    def _get_parameter(self, name: str, decrypt: bool = False) -> str:
        try:
            response = self.ssm.get_parameter(Name=name, WithDecryption=decrypt)
        except ClientError:
            # logger.warning("Problem calling the parameter store for the key '%s'", name)
            raise
        return response["Parameter"]["Value"]

    def get_param(self, param: str) -> str:
        return self._get_parameter(param)

    def get_secure_param(self, param: str) -> str:
        return self._get_parameter(param, decrypt=True)

    def set_param(self, param: str, value: str, overwrite: bool = False) -> None:
        self.ssm.put_parameter(Name=param, Value=value,
                               Type='String', Overwrite=overwrite)

    def set_secure_param(self, param: str, value: str, overwrite: bool = False) -> None:
        self.ssm.put_parameter(Name=param, Value=value,
                               Type='SecureString', Overwrite=overwrite)
