from __future__ import annotations

import asyncio
import copy
import time
import warnings
from typing import Any, Optional, Union, TypeVar, Generic, Sequence

T = TypeVar("T")
Shared = dict[str, Any]
Params = dict[str, Any]


class BaseNode:
    def __init__(self) -> None:
        self.params: Params = {}
        self.successors: dict[str, BaseNode] = {}

    def set_params(self, params: Params) -> None:
        self.params = params

    # TODO: Invoked by using >> operator (e.g. node1 >> node2, action will be "default")
    # TODO: This registers provided node as a successor of this node using action as key to ensure 1 action can only refer to 1 successor.
    def next(self, node: BaseNode, action: str = "default") -> BaseNode:
        if action in self.successors:
            warnings.warn(f"Overwriting successor for action '{action}'")
        self.successors[action] = node
        return node

    def prep(self, shared: Shared) -> Any:
        pass

    def exec(self, prep_res: Any) -> Any:
        pass

    def post(self, shared: Shared, prep_res: Any, exec_res: Any) -> Any:
        pass

    def _exec(self, prep_res: Any) -> Any:
        return self.exec(prep_res)

    # TODO: Prep -> Exec -> Post | Action (run result) returns.
    def _run(self, shared: Shared) -> Any:
        p = self.prep(shared)
        e = self._exec(p)
        return self.post(shared, p, e)

    def run(self, shared: Shared) -> Any:
        if self.successors:
            warnings.warn("Node won't run successors. Use Flow.")
        return self._run(shared)

    def __rshift__(self, other: BaseNode) -> BaseNode:
        return self.next(other)

    # TODO: Register a successor for this node using a CUSTOM ACTION as key. When flow runs, it will use this action to look up node's successors to determine which successor to run.
    def __sub__(self, action: str) -> _ConditionalTransition:
        if isinstance(action, str):
            # TODO: Instantiate a ConditionalTransition node with __init__(src=self, action=action)
            return _ConditionalTransition(self, action)
        raise TypeError("Action must be a string")


# TODO: Register a successor for this node using a CUSTOM ACTION as key. When flow runs, it will use this action to look up node's successors to determine which successor to run.
# TODO: Usage example: a _ConditionalTransition.__rshift__(b) ----Eventually equal to----> a.next(target=b, action="ok") ----Leads to---> a.successors["ok"] = b
class _ConditionalTransition:
    def __init__(self, src: BaseNode, action: str) -> None:
        self.src = src
        self.action = action

    def __rshift__(self, tgt: BaseNode) -> BaseNode:
        # TODO: Invoke next. Which will register tgt as a successor of src using the PROVIDED ACTION as key to ensure 1 action can only refer to 1 successor.
        return self.src.next(tgt, self.action)


class Node(BaseNode):
    def __init__(self, max_retries: int = 1, wait: float = 0) -> None:
        super().__init__()
        self.max_retries = max_retries
        self.wait = wait

    def exec_fallback(self, prep_res: Any, exc: Exception) -> Any:
        raise exc

    def _exec(self, prep_res: Any) -> Any:
        for self.cur_retry in range(self.max_retries):
            try:
                return self.exec(prep_res)
            except Exception as e:
                if self.cur_retry == self.max_retries - 1:
                    return self.exec_fallback(prep_res, e)
                if self.wait > 0:
                    time.sleep(self.wait)


# TODO: Node that can run the _exec() against x input items in sequence.
class BatchNode(Node):
    def _exec(self, items: Optional[Sequence[Any]]) -> list[Any]:
        return [super()._exec(i) for i in (items or [])]


class Flow(BaseNode):
    def __init__(self, start: Optional[BaseNode] = None) -> None:
        super().__init__()
        self.start_node = start

    def start(self, start: BaseNode) -> BaseNode:
        self.start_node = start
        return start

    def get_next_node(
        self, curr: BaseNode, action: Optional[str]
    ) -> Optional[BaseNode]:
        # TODO: Try to get next node by action from the registered successors of current node.
        nxt = curr.successors.get(action or "default")
        # TODO: No successor found for this current node, traversal ends.
        if not nxt and curr.successors:
            warnings.warn(f"Flow ends: '{action}' not found in {list(curr.successors)}")
        return nxt

    def _orch(self, shared: Shared, params: Optional[Params] = None) -> Any:
        # TODO: Orchestration starts from start node (set by start method)
        curr = copy.copy(self.start_node)
        p = params or {**self.params}
        last_action = None
        # TODO: Traverse nodes until there is no next node
        while curr:
            curr.set_params(p)
            # TODO: Run current node & pass shared state
            # TODO: Last action would be pass to next node. Node like ConditionalTransition can be use that to decide if it should stop or continue.
            last_action = curr._run(shared)
            curr = copy.copy(self.get_next_node(curr, last_action))
        return last_action

    def _run(self, shared: Shared) -> Any:
        p = self.prep(shared)
        o = self._orch(shared)
        return self.post(shared, p, o)

    def post(self, shared: Shared, prep_res: Any, exec_res: Any) -> Any:
        return exec_res


class BatchFlow(Flow):
    def _run(self, shared: Shared) -> Any:
        pr = self.prep(shared) or []
        for bp in pr:
            self._orch(shared, {**self.params, **bp})
        return self.post(shared, pr, None)


class AsyncNode(Node):
    async def prep_async(self, shared: Shared) -> Any:
        pass

    async def exec_async(self, prep_res: Any) -> Any:
        pass

    async def exec_fallback_async(self, prep_res: Any, exc: Exception) -> Any:
        raise exc

    async def post_async(self, shared: Shared, prep_res: Any, exec_res: Any) -> Any:
        pass

    async def _exec(self, prep_res: Any) -> Any:
        for i in range(self.max_retries):
            try:
                return await self.exec_async(prep_res)
            except Exception as e:
                if i == self.max_retries - 1:
                    return await self.exec_fallback_async(prep_res, e)
                if self.wait > 0:
                    await asyncio.sleep(self.wait)

    async def run_async(self, shared: Shared) -> Any:
        if self.successors:
            warnings.warn("Node won't run successors. Use AsyncFlow.")
        return await self._run_async(shared)

    async def _run_async(self, shared: Shared) -> Any:
        p = await self.prep_async(shared)
        e = await self._exec(p)
        return await self.post_async(shared, p, e)

    def _run(self, shared: Shared) -> Any:
        raise RuntimeError("Use run_async.")


# TODO: (Async) Node that can run the _exec() against x input items in sequence.
class AsyncBatchNode(AsyncNode, BatchNode):
    async def _exec(self, items: Sequence[Any]) -> list[Any]:
        return [await super()._exec(i) for i in items]


# TODO: (Async) Node that can run the _exec() against x input items.
# TODO: Execute _exec() concurrently.
class AsyncParallelBatchNode(AsyncNode, BatchNode):
    async def _exec(self, items: Sequence[Any]) -> list[Any]:
        return await asyncio.gather(*(super()._exec(i) for i in items))


class AsyncFlow(Flow, AsyncNode):
    async def _orch_async(self, shared: Shared, params: Optional[Params] = None) -> Any:
        curr = copy.copy(self.start_node)
        p = params or {**self.params}
        last_action = None
        while curr:
            curr.set_params(p)
            if isinstance(curr, AsyncNode):
                last_action = await curr._run_async(shared)
            else:
                last_action = curr._run(shared)
            curr = copy.copy(self.get_next_node(curr, last_action))
        return last_action

    async def _run_async(self, shared: Shared) -> Any:
        p = await self.prep_async(shared)
        o = await self._orch_async(shared)
        return await self.post_async(shared, p, o)

    async def post_async(self, shared: Shared, prep_res: Any, exec_res: Any) -> Any:
        return exec_res


class AsyncBatchFlow(AsyncFlow, BatchFlow):
    async def _run_async(self, shared: Shared) -> Any:
        pr = await self.prep_async(shared) or []
        for bp in pr:
            await self._orch_async(shared, {**self.params, **bp})
        return await self.post_async(shared, pr, None)


class AsyncParallelBatchFlow(AsyncFlow, BatchFlow):
    async def _run_async(self, shared: Shared) -> Any:
        pr = await self.prep_async(shared) or []
        await asyncio.gather(
            *(self._orch_async(shared, {**self.params, **bp}) for bp in pr)
        )
        return await self.post_async(shared, pr, None)
