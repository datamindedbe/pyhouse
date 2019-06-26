from abc import ABC, abstractmethod


class ParameterStore(ABC):
    @abstractmethod
    def get_param(self, param):
        raise NotImplementedError

    @abstractmethod
    def get_secure_param(self, param):
        raise NotImplementedError

    @abstractmethod
    def set_param(self, param, value, overwrite=False):
        raise NotImplementedError

    @abstractmethod
    def set_secure_param(self, param, value, overwrite=False):
        raise NotImplementedError
