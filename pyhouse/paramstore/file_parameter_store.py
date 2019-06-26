from configparser import ConfigParser

from .parameter_store import ParameterStore


class FileParameterStore(ParameterStore):
    def __init__(self, filename: str):
        self.filename = filename
        self.config = ConfigParser()

    def get_param(self, param: str) -> str:
        self.config.read(self.filename)
        return self.config["config"][param]

    def set_param(
        self, param: str, value: str, overwrite: bool = False, secure: bool = False
    ) -> None:
        self.config.read(self.filename)
        if overwrite:
            self.config["config"][param] = value
        else:
            self.config["config"].setdefault("param", value)
        with open(self.filename, "w") as out:
            self.config.write(out)
