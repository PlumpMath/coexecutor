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

    async def _launcher(self, coro):
        ret = await coro
        self._current_workers -= 1
        while self._current_workers < self._max_workers:
            waiting_future = None
            while len(self._pending_queue) > 0:
                generator = self._pending_queue[0]
                try:
                    waiting_future = next(generator)
                    break
                except StopIteration:
                    self._pending_queue.popleft()
                    continue
            if waiting_future is None:
                break

            self._current_workers += 1
            asyncio.ensure_future(waiting_future, loop=self._loop)
        if self._idle():
            async with self._waiter:
                self._waiter.notify_all()
        return ret

    @overrides
    def submit(self, coroutine_function, *args, **kwargs) -> asyncio.Future:
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")

        if "loop" in kwargs:
            if kwargs["loop"] is not self._loop:
                raise RuntimeError("Loop mismatch. Definitely a bug.")
        else:
            kwargs["loop"] = self._loop

        future = self._launcher(coroutine_function(*args, **kwargs))

        if self._current_workers < self._max_workers:
            self._current_workers += 1
            asyncio.ensure_future(future, loop=self._loop)
        else:
            def _single_generator():
                yield future
            self._pending_queue.append(_single_generator())
        return future

    def _idle(self):
        return self._current_workers == 0 and len(self._pending_queue) == 0

    class _async_generator():
        def __init__(self, loop, limit=None):
            if limit is None:
                limit = 0
            self._queue = asyncio.Queue(maxsize=limit, loop=loop)
            self._stopped = False
            self._waiter = None

        async def put(self, item):
            await self._queue.put(item)

        def stop(self):
            if not self._stopped:
                self._stopped = True
                if self._queue.empty() and self._waiter is not None:
                    self._waiter.cancel()

        async def __aiter__(self):
            return self

        async def __anext__(self):
            if self._stopped and self._queue.empty():
                raise StopAsyncIteration
            future = self._queue.get()
            self._waiter = future
            try:
                return await future
            except asyncio.CancelledError:
                self._waiter = None
                raise StopAsyncIteration


    @overrides
    def map(self, coroutine_function, *iterables, limit=None):
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")

        queue = self._async_generator(loop=self._loop, limit=limit)

        def _coroutine_generator():
            async def _wrapper(arg):
                ret = await coroutine_function(*arg, loop=self._loop)
                await queue.put(ret)
            for arg in zip(*iterables):
                yield self._launcher(_wrapper(arg))
            queue.stop()

        generator = _coroutine_generator()

        while self._current_workers < self._max_workers:
            self._current_workers += 1
            try:
                current_coro = next(generator)
                asyncio.ensure_future(current_coro, loop=self._loop)
            except StopIteration:
                return queue

        self._pending_queue.append(generator)
        return queue

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

import time
from concurrent.futures import ThreadPoolExecutor
def main():
    async def test(index, index2, loop):
        await asyncio.sleep(1, loop=loop)
        print("test", index, index2)
        return (index,index2)

    def test_thread(index, index2):
        time.sleep(1)
        print("test", index, index2)
        return (index, index2)


    loop = asyncio.new_event_loop()

    async def async_main(loop):
        async with CoroutinePoolExecutor(loop=loop, max_workers=4, debug=True) as coex:
            async for ret in coex.map(test, range(10), range(10,20)):
                print(ret)

    try:
        loop.run_until_complete(async_main(loop=loop))
    finally:
        loop.close()

    with ThreadPoolExecutor(max_workers=4) as exe:
        for x in exe.map(test_thread, range(10), range(10,20)):
            print(x)

if __name__ == "__main__":
    main()
