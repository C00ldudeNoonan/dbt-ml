from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from graphlib import CycleError, TopologicalSorter

from .config.model import ModelConfig
from .config.source import SourceConfig

_REF_PATTERN = re.compile(r"^\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*$")


class NodeKind(StrEnum):
    SOURCE = "source"
    MODEL = "model"


@dataclass(frozen=True)
class Node:
    name: str
    kind: NodeKind
    tags: frozenset[str] = frozenset()


class DAGError(Exception):
    pass


class SelectionError(Exception):
    pass


def parse_ref(value: str) -> str:
    match = _REF_PATTERN.match(value)
    if match:
        return match.group(1)
    return value.strip()


def _bfs(start: str, adjacency: dict[str, set[str]]) -> set[str]:
    """Return all nodes reachable from `start` (not including `start` itself)."""
    visited: set[str] = set()
    queue = list(adjacency.get(start, set()))
    while queue:
        node = queue.pop()
        if node in visited:
            continue
        visited.add(node)
        queue.extend(adjacency.get(node, set()))
    return visited


class ProjectDAG:
    def __init__(self, sources: list[SourceConfig], models: list[ModelConfig]) -> None:
        self.nodes: dict[str, Node] = {}
        self.predecessors: dict[str, set[str]] = {}
        self.successors: dict[str, set[str]] = {}

        for source in sources:
            self._add_node(Node(source.name, NodeKind.SOURCE, frozenset(source.tags)))

        for model in models:
            self._add_node(Node(model.name, NodeKind.MODEL, frozenset(model.tags)))
            preds: set[str] = set()
            if model.source:
                preds.add(parse_ref(model.source))
            if model.depends_on:
                preds.update(parse_ref(dep) for dep in model.depends_on)
            self.predecessors[model.name] = preds

        for model_name, preds in self.predecessors.items():
            for pred in preds:
                if pred not in self.nodes:
                    raise DAGError(
                        f"Model '{model_name}' references unknown node '{pred}'"
                    )
                self.successors[pred].add(model_name)

        try:
            self._sorted: list[str] = list(
                TopologicalSorter(self.predecessors).static_order()
            )
        except CycleError as e:
            cycle = " -> ".join(e.args[1]) if len(e.args) > 1 else str(e.args)
            raise DAGError(f"Cyclic dependency detected: {cycle}") from e

    def _add_node(self, node: Node) -> None:
        if node.name in self.nodes:
            raise DAGError(f"Duplicate node name: {node.name}")
        self.nodes[node.name] = node
        self.predecessors.setdefault(node.name, set())
        self.successors.setdefault(node.name, set())

    def execution_order(self) -> list[str]:
        return [name for name in self._sorted if self.nodes[name].kind == NodeKind.MODEL]

    def select_models(
        self, *, select: str | None = None, exclude: str | None = None
    ) -> list[str]:
        """Resolve selector expressions to model names, in topological order.

        Selector syntax (whitespace-separated tokens):
          - `name`        — just that node
          - `+name`       — name plus all transitive ancestors
          - `name+`       — name plus all transitive descendants
          - `+name+`      — name plus ancestors and descendants
        Source nodes can match but are never returned (sources don't run).
        """
        if select:
            selected = self._apply(select)
        else:
            selected = set(self.nodes)

        if exclude:
            selected -= self._apply(exclude)

        return [
            n
            for n in self._sorted
            if n in selected and self.nodes[n].kind == NodeKind.MODEL
        ]

    def _apply(self, expression: str) -> set[str]:
        out: set[str] = set()
        for token in expression.split():
            out |= self._expand_token(token)
        return out

    def _expand_token(self, token: str) -> set[str]:
        up = token.startswith("+")
        down = token.endswith("+")
        body = token.strip("+")
        if not body:
            raise SelectionError(f"Empty selector token in '{token}'")

        if body.startswith("tag:"):
            tag = body[4:]
            if not tag:
                raise SelectionError(f"Empty tag in selector '{token}'")
            seeds = {n for n, node in self.nodes.items() if tag in node.tags}
            if not seeds:
                raise SelectionError(
                    f"No nodes tagged '{tag}'. Known tags: {sorted(self._all_tags())}"
                )
        else:
            if body not in self.nodes:
                raise SelectionError(
                    f"Unknown selector '{body}'. Known nodes: {sorted(self.nodes)}"
                )
            seeds = {body}

        result: set[str] = set(seeds)
        for seed in seeds:
            if up:
                result |= _bfs(seed, self.predecessors)
            if down:
                result |= _bfs(seed, self.successors)
        return result

    def _all_tags(self) -> set[str]:
        out: set[str] = set()
        for node in self.nodes.values():
            out |= node.tags
        return out

    def all_nodes_in_order(self) -> list[Node]:
        return [self.nodes[name] for name in self._sorted]

    def to_mermaid(self) -> str:
        lines = ["graph LR"]
        connected: set[str] = set()
        for name, preds in self.predecessors.items():
            for pred in preds:
                lines.append(f"    {pred} --> {name}")
                connected.add(pred)
                connected.add(name)
        for name in self.nodes:
            if name not in connected:
                lines.append(f"    {name}")
        return "\n".join(lines)
