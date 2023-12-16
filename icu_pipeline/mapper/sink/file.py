import pandas as pd
from pandera.typing import DataFrame

from icu_pipeline.mapper.schema import AbstractSinkSchema
from icu_pipeline.mapper.sink import AbstractSinkMapper


class CSVFileSinkMapper(AbstractSinkMapper):
    def to_output_format(
        self,
        df: DataFrame[AbstractSinkSchema],
        schema: AbstractSinkSchema,
    ) -> None:
        for column in df.columns:
            if not isinstance(df[column][0], dict):
                continue

            df = df.join(pd.json_normalize(df[column]).add_prefix(f"{column}__"))
            df = df.drop(columns=[column])

        df.to_csv(f"{schema._SINK_NAME}.csv", mode="a+", index=False)
