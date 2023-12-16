import pandera as pa


class AbstractSinkSchema(pa.DataFrameModel):
    _SINK_NAME: str
