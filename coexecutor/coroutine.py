from coexecutor._base import *
import asyncio


class CoroutinePoolExecutor(CoExecutor):
    def __init__(self, loop: asyncio.AbstractEventLoop = None):
        if loop is None:
            loop = asyncio.get_event_loop_policy().get_event_loop()
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError("Must provide an EventLoop")
        self._loop = loop

    @overrides
    def submit(self, coroutine_function, *args, **kwargs) -> asyncio.Future:
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        raise NotImplementedError()

    @overrides
    def map(self, coroutine_function, *iterables, timeout=None):
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        raise NotImplementedError()

    @overrides
    async def shutdown(self, wait=True):
        raise NotImplementedError()

    pass
