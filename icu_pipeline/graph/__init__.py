import os
from enum import StrEnum, auto


class GraphType(StrEnum):
    InMemory = auto()
    Multiprocessing = auto()


t = os.environ.get("GRAPH_TYPE", GraphType.InMemory)
match t:
    case GraphType.InMemory:
        from icu_pipeline.graph.in_memory import InMemoryNode as Node
        from icu_pipeline.graph.in_memory import InMemoryPipe as Pipe
    case GraphType.Multiprocessing:
        from icu_pipeline.graph.parallel import MultiprocessingNode as Node
        from icu_pipeline.graph.parallel import MultiprocessingPipe as Pipe
    case _:
        raise EnvironmentError(f"Unknown GraphType '{t}'. Available Modules: {[tt.value for tt in GraphType]}")

__all__ = ["Node", "Pipe"]
