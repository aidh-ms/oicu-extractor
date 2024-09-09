from pandas.core.api import DataFrame as DataFrame
from icu_pipeline.graph.base import BasePipe, BaseNode
from icu_pipeline.job import Job


class InMemoryNode(BaseNode):
    def fetch_sources(self, job: Job, *args, **kwargs) -> dict[str, DataFrame]:
        out: dict[str, DataFrame] = {}
        for c,s in self._sources.items():
            out[c] = s.read(job, *args, **kwargs)
        return out


class InMemoryPipe(BasePipe):
    def __init__(self, source: InMemoryNode, sink: InMemoryNode) -> None:
        super().__init__(source, sink)

    def read(self, job: Job, *args, **kwargs):
        # Forward the get_data method
        return self._source.get_data(job)

    def write(self, job: Job, data, *args, **kwargs):
        # Nothing special
        return data
