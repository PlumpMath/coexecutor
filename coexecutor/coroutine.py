from coexecutor._base import *
import asyncio
import typing
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
            ret = await coroutine_function(*args, **kwargs)
            self._current_workers -= 1
            while len(self._pending_queue) > 0 and self._current_workers < self._max_workers:
                waiting_future = self._pending_queue.popleft()
                self._current_workers += 1
                asyncio.ensure_future(waiting_future, loop=self._loop)
            return ret
        future = _launcher()

        if self._current_workers < self._max_workers:
            self._current_workers += 1
            asyncio.ensure_future(future, loop=self._loop)
        else:
            self._pending_queue.append(future)
        return future

    @overrides
    def map(self, coroutine_function, *iterables, timeout=None):
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")
        raise NotImplementedError()

    @overrides
    async def shutdown(self, wait=True):
        raise NotImplementedError()

    pass


def main():
    coex = CoroutinePoolExecutor()
    loop = asyncio.get_event_loop()

    async def waiter(loop):
        try:
            print("enter waiter")
            await asyncio.sleep(10,loop=loop)
            print("sleep waiter")
        except asyncio.CancelledError as e:
            print("waiter canceled", e)
        except Exception as e:
            print("waiter", e)
        finally:
            loop.stop()
            print("end waiter")

    future = asyncio.ensure_future(waiter(loop))

    async def waiter2(loop):
        try:
            await asyncio.sleep(1,loop=loop)
            future.cancel()
        except Exception as e:
            print("waiter2", e)
        finally:
            pass

    asyncio.ensure_future(waiter2(loop))
    try:
        loop.run_forever()
    except Exception as e:
        print("main", e)

if __name__ == "__main__":
    main()
