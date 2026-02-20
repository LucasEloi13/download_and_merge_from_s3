from abc import ABC, abstractmethod
from typing import Any, Generic, List, TypeVar

T = TypeVar("T")


class DataSource(ABC, Generic[T]):

    @abstractmethod
    def connect(self) -> T:
        raise NotImplementedError

    @abstractmethod
    def execute_query(self, query: str, params: dict = None) -> Any:
        raise NotImplementedError

    @abstractmethod
    def fetch_object(self, key: str) -> Any:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError
