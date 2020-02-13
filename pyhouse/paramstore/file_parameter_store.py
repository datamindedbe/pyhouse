from configparser import ConfigParser
from pathlib import Path
from typing import Union

from .parameter_store import ParameterStore


class FileParameterStore(ParameterStore):
    def __init__(self, filename: Union[str, Path]):
        self.filename = Path(filename) if isinstance(filename, str) else filename
        self.config = ConfigParser()
        self._section = "config"

    def get_param(self, param: str) -> str:
        self.config.read(self.filename)
        return self.config[self._section][param]

    def set_param(
        self, param: str, value: str, overwrite: bool = False, secure: bool = False
    ) -> None:
        self.config.read(self.filename)
        if self._section not in self.config:
            self.config[self._section] = {}

        if overwrite:
            self.config[self._section][param] = value
        else:
            self.config[self._section].setdefault(param, value)

        with open(self.filename, "w") as out:
            self.config.write(out)
