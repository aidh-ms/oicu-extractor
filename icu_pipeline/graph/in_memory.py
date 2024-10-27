from typing import Any

from pandera.typing import DataFrame

from icu_pipeline.graph.base import BaseNode, BasePipe
from icu_pipeline.job import Job


class InMemoryNode(BaseNode):
    def fetch_sources(self, job: Job, *args: list[Any], **kwargs: dict[Any, Any]) -> dict[str, DataFrame]:
        out: dict[str, DataFrame] = {}
        for c, s in self._sources.items():
            out[c] = s.read(job, *args, **kwargs)
        return out


class InMemoryPipe(BasePipe):
    def __init__(self, source: InMemoryNode, sink: InMemoryNode) -> None:
        super().__init__(source, sink)

    def read(self, job: Job, *args: list[Any], **kwargs: dict[Any, Any]) -> DataFrame:
        # Forward the get_data method
        return self._source.get_data(job)

    def write(self, job: Job, data: DataFrame, *args: list[Any], **kwargs: dict[Any, Any]) -> None:
        # Nothing special
        return data  # type: ignore[return-value]  # TODO
