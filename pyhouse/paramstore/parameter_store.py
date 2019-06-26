from abc import ABC, abstractmethod


class ParameterStore(ABC):
    @abstractmethod
    def get_param(self, param: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def set_param(
        self, param: str, value: str, overwrite: bool = False, secure: bool = False
    ) -> None:
        raise NotImplementedError
