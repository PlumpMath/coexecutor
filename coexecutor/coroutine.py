from coexecutor._base import *
import asyncio
from collections import deque


class CoroutinePoolExecutor(CoExecutor):
    def __init__(self, max_workers: int = 1, loop: asyncio.AbstractEventLoop = None, debug=False):
        if not max_workers or not isinstance(max_workers, int) or max_workers < 1:
            raise TypeError("Expect integer greater than 0")
        if loop is None:
            loop = asyncio.get_event_loop_policy().get_event_loop()
        if not isinstance(loop, asyncio.AbstractEventLoop):
            raise TypeError("Must provide an EventLoop")
        self._loop = loop

        self._current_workers = 0
        self._max_workers = max_workers
        self._futures_to_wait = dict()
        self._debug = debug
        self._pending_queue = deque()
        self._shutdowned = False
        self._waiter = asyncio.Condition(loop=self._loop)

        if self._debug:
            async def _thread_monitor():
                nonlocal self
                try:
                    while self._current_workers > 0:
                        print("Current tasks: %d" % self._current_workers)
                        await asyncio.sleep(1, loop=self._loop)

                except asyncio.CancelledError:
                    print("All tasks are done! [%d]" % self._current_workers)
                    del self._futures_to_wait["thread_monitor"]

            self._futures_to_wait["thread_monitor"] = asyncio.ensure_future(_thread_monitor(), loop=self._loop)

    @overrides
    def submit(self, coroutine_function, *args, **kwargs) -> asyncio.Future:
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")

        async def _launcher():
            if "loop" in kwargs:
                if kwargs["loop"] is not self._loop:
                    raise RuntimeError("Loop mismatch. Definitely a bug.")
            else:
                kwargs["loop"] = self._loop
            ret = await coroutine_function(*args, **kwargs)
            self._current_workers -= 1
            while len(self._pending_queue) > 0 and self._current_workers < self._max_workers:
                waiting_future = self._pending_queue.popleft()
                self._current_workers += 1
                asyncio.ensure_future(waiting_future, loop=self._loop)
            if self._idle():
                async with self._waiter:
                    self._waiter.notify_all()
            return ret
        future = _launcher()

        if self._current_workers < self._max_workers:
            self._current_workers += 1
            asyncio.ensure_future(future, loop=self._loop)
        else:
            self._pending_queue.append(future)
        return future

    def _idle(self):
        return self._current_workers == 0 and len(self._pending_queue) == 0

    @overrides
    def map(self, coroutine_function, *iterables, timeout=None):
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")
        raise NotImplementedError()

    @overrides
    async def shutdown(self, wait=True):
        self._shutdowned = True

        async def _cancel_futures():
            try:
                async with self._waiter:
                    await self._waiter.wait_for(self._idle)
                    for (key, future) in self._futures_to_wait.items():
                        future.cancel()
            except asyncio.CancelledError:
                pass

        if wait:
            await _cancel_futures()
        else:
            asyncio.ensure_future(_cancel_futures(), loop=self._loop)


def main():
    async def test(loop):
        await asyncio.sleep(1, loop=loop)
        print("hello")


    loop = asyncio.new_event_loop()
    coex = CoroutinePoolExecutor(loop=loop, debug=True)
    coex.submit(test)
    coex.submit(test)
    coex.submit(test)
    coex.submit(test)

    async def async_main(loop):
        print("ha1")
        await coex.shutdown(wait=False)
        await asyncio.sleep(3, loop=loop)
        print("ha2")
        await coex.shutdown(wait=True)
        print("ha3")

    async def async_main2(loop):
        print("ho1")
        await asyncio.sleep(2, loop=loop)
        await coex.shutdown(wait=True)
        await coex.shutdown(wait=False)
        print("ho2")
        await coex.shutdown(wait=True)
        print("ho3")

    try:
        #loop.run_until_complete(async_main(loop=loop))
        asyncio.ensure_future(async_main(loop=loop), loop=loop)
        asyncio.ensure_future(async_main2(loop=loop), loop=loop)
        loop.run_forever()
    finally:
        loop.close()

if __name__ == "__main__":
    main()
