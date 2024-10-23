from typing import Any

from pandera.typing import DataFrame

from icu_pipeline.graph import Node
from icu_pipeline.job import Job


class PandasSink(Node):
    def __init__(self) -> None:
        super().__init__("concept_id")

    def get_data(self, job: Job, *args: list[Any], **kwargs: dict[Any, Any]) -> DataFrame:
        data = self.fetch_sources(job, *args, **kwargs)
        # TODO - Merge the Data?
        return data  # type: ignore[return-value]  # TODO
