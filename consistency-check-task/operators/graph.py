from universalis.common.local_state_backends import LocalStateBackend
from universalis.common.stateflow_graph import StateflowGraph

from .source import source_operator
from .sink import sink_operator
from .task import task_operator

g = StateflowGraph('task-demo', operator_state_backend=LocalStateBackend.DICT)
