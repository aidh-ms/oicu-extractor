from pandera.typing import DataFrame
from ..schema import AbstractSinkSchema
from icu_pipeline.job import Job
from icu_pipeline.sink import AbstractSinkMapper
from icu_pipeline.graph import Node


class PandasSink(Node):
    def __init__(self) -> None:
        super().__init__(None)

    def get_data(self, job: Job, *args, **kwargs):
        data = self.fetch_sources(job, *args, **kwargs)
        # TODO - Merge the Data?
        return data
