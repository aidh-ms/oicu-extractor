from typing import TYPE_CHECKING, Any

from pandera.typing import DataFrame

from icu_pipeline.job import Job

if TYPE_CHECKING:
    from icu_pipeline.concept import Concept


class BaseNode:
    ID = 0
    REQUIRED_CONCEPTS: list["Concept"] = []

    def __init__(self, concept_id: str) -> None:
        self._node_id = BaseNode.ID
        BaseNode.ID += 1
        # Maps from concept_id --> Pipe
        self._concept_id: str = concept_id
        self._sources: dict[str, BasePipe] = {}
        self._sinks: dict[str, BasePipe] = {}

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, BaseNode):
            return False
        n: BaseNode = value
        # Maybe different check? But it would require more attributes for Node..
        return n._node_id == self._node_id

    def __str__(self) -> str:
        return f"{type(self).__name__}({self._node_id})"

    def fetch_sources(self, job: Job, *args: Any, **kwargs: Any) -> dict[str, DataFrame]:
        raise NotImplementedError

    def get_data(self, job: Job, *args: Any, **kwargs: Any) -> DataFrame:
        raise NotImplementedError


class BasePipe:
    def __init__(self, source: BaseNode, sink: BaseNode) -> None:
        self._source = source
        self._sink = sink

    def write(self, job: Job, data: DataFrame, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def read(self, job: Job, *args: Any, **kwargs: Any) -> DataFrame:
        raise NotImplementedError


class Graph:
    def __init__(self) -> None:
        self._nodes: list[BaseNode] = []
        self._edges: list[BasePipe] = []

    @property
    def sources(self) -> list[BaseNode]:
        out = []
        for n in self._nodes:
            if len(n._sources) == 0 and n not in out:
                out.append(n)
        return out

    @property
    def sinks(self) -> list[BaseNode]:
        out = []
        for n in self._nodes:
            if len(n._sinks) == 0 and n not in out:
                out.append(n)
        return out

    def getNode(self, node: BaseNode | str) -> BaseNode | None:
        for n in self._nodes:
            if n == node:
                return n
        return None

    def addPipe(self, source: BaseNode, sink: BaseNode) -> None:
        from icu_pipeline.graph import Pipe

        # Check if the Nodes are already part of the Graph
        if source not in self._nodes:
            self._nodes.append(source)
        if sink not in self._nodes:
            self._nodes.append(sink)

        new_pipe = Pipe(source, sink)  # type: ignore[arg-type]
        # Append the Pipe to the Graph
        self._edges.append(new_pipe)
        # Append the Pipe to the Nodes
        source._sinks[source._concept_id] = new_pipe
        sink._sources[source._concept_id] = new_pipe

        self.check_circularity()

    def check_circularity(self) -> bool:
        """Only works for directed acyclic graphs (DAGs).
        Return 'True' if the check is passed"""
        # Assume everythings fine
        result = True

        if len(self.sinks) == 0 and len(self._nodes) > 0:
            result = False
        if len(self.sources) == 0 and len(self._nodes) > 0:
            result = False

        def _is_circular(node: BaseNode, nodes: list[BaseNode]) -> bool:
            # Check if I'm part of the visited Nodes
            if node in nodes:
                return True
            # If not, add me to the list and repeat for my sources
            for next_source in node._sources.values():
                if _is_circular(next_source._source, [node, *nodes]):
                    return True
            # If it didn't break to this point, it's not circular
            return False

        # Sinks are fetched during execution, so begin there
        for next_sink in self.sinks:
            # For all possible sub-paths in this sink, check if a node appears twice
            if _is_circular(next_sink, []):
                result = False

        assert result, "Graph has Circular dependencies!"
        return result

    def check_hanging_leaves(self) -> bool:
        """Are there any Nodes that are not connected to the sink?"""
        raise NotImplementedError

    def check_missing_connections(self) -> bool:
        """Are there any dependencies not met?"""
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        from icu_pipeline.graph import Node, Pipe

        def _get_sink_str(n: BaseNode) -> list[str]:
            if len(n._sinks) == 0:
                return [str(n)]
            out = []
            for next_sink in n._sinks.values():
                out.extend(_get_sink_str(next_sink._sink))
            out = [f"{n}-->{s}" for s in out]
            return out

        out = [f"Pipe Class: {Pipe.__name__}", f"Node Class: {Node.__name__}"]
        for next_source in self.sources:
            out.extend(_get_sink_str(next_source))
        return "\n".join(out)
