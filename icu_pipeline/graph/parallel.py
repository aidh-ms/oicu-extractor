import multiprocessing
from typing import Any

from pandera.typing import DataFrame

from icu_pipeline.graph.base import BaseNode, BasePipe
from icu_pipeline.job import Job
from icu_pipeline.logger import ICULogger

logger = ICULogger.get_logger()


class MultiprocessingNode(BaseNode):
    def fetch_sources(self, job: Job, *args: list[Any], **kwargs: dict[Any, Any]) -> dict[str, DataFrame]:
        manager = multiprocessing.Manager()
        out = manager.dict()

        logger.debug(f"Getting data for Node '{self}'...")
        procs = [s.read(job, out) for s in self._sources.values()]
        for p in procs:
            p.join()
        return out

    def get_data(self, job: Job, *args: list[Any], **kwargs: dict[Any, Any]) -> DataFrame:
        data = self.fetch_sources(job)
        if self._concept_id is not None:
            data = data[self._concept_id]
        return data


class MultiprocessingPipe(BasePipe):
    def __init__(self, source: MultiprocessingNode, sink: MultiprocessingNode) -> None:
        super().__init__(source, sink)

    def read(self, job: Job, managed_dict, *args: list[Any], **kwargs: dict[Any, Any]) -> DataFrame:
        def _read(result: dict):
            df = self._source.get_data(job)
            result[self._source._concept_id] = df

        p = multiprocessing.Process(target=_read, args=[managed_dict], daemon=False)
        p.start()
        return p

    def write(self, job: Job, data: DataFrame, *args: list[Any], **kwargs: dict[Any, Any]) -> None:
        # Nothing special
        return data
