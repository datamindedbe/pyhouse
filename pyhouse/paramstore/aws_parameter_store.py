import logging
import boto3

from .parameter_store import ParameterStore

logger = logging.getLogger(__name__)


class AwsParameterStore(ParameterStore):
    """
    Retrieve or store values from/into the AWS Parameter Store.
    """

    def __init__(self, region_name: str = "eu-west-1"):
        self.ssm = boto3.client("ssm", region_name=region_name, verify=None)

    def get_param(self, name: str) -> str:
        # aws will randomly drop ssm requests
        try:
            value = self.ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
            return str(value)
        except Exception as e:
            logger.warning(f"AwsParameterStore::get_param:{name}: Caught exception {e}")
            raise e

    def set_param(
        self, param: str, value: str, overwrite: bool = False, secure: bool = False
    ) -> None:
        param_type = "SecureString" if secure else "String"
        try:
            # aws will randomly drop ssm requests
            self.ssm.put_parameter(Name=param, Value=value, Type=param_type, Overwrite=overwrite)
        except Exception as e:
            logger.warning("AwsParameterStore::set_param: Caught exception")
            raise e
