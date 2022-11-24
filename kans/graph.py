from typing import Dict, List, Type
from uuid import uuid4
from kans.exceptions import CircularDependencyError, MissingDependencyError
from kans.tasks import Task
from networkx import (
    MultiDiGraph,
    compose,
    is_directed_acyclic_graph,
    is_weakly_connected,
    number_of_nodes,
    is_empty,
    topological_sort
)