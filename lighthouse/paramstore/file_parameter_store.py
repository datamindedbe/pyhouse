from .parameter_store import ParameterStore
from configparser import ConfigParser


class FileParameterStore(ParameterStore):

    def __init__(self, filename):
        self.filename = filename
        self.config = ConfigParser()

    def get_param(self, param):
        self.config.read(self.filename)
        return self.config['config'][param]

    def get_secure_param(self, param):
        return self.get_param(param)

    def set_param(self, param, value, overwrite=False):
        self.config.read(self.filename)
        if overwrite:
            self.config['config'][param] = value
        else:
            self.config["config"].setdefault("param", value)
        with open(self.filename, 'w') as out:
            self.config.write(out)

    def set_secure_param(self, param, value, overwrite=False):
        self.set_param(param, value)
