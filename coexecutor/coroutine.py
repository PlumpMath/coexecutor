from coexecutor._base import *
import asyncio
import collections
import enum


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
        self._pending_queue = collections.deque()
        self._shutdowned = False
        self._waiter = asyncio.Condition(loop=self._loop)

        if self._debug:
            async def _thread_monitor():
                try:
                    while True:
                        if self._current_workers > 0:
                            print("Current tasks: %d" % self._current_workers)
                        await asyncio.sleep(1, loop=self._loop)

                except asyncio.CancelledError:
                    print("All tasks are done! [%d]" % self._current_workers)

            self._futures_to_wait["thread_monitor"] = asyncio.ensure_future(_thread_monitor(), loop=self._loop)

    async def _launcher(self, coro, callback_future: asyncio.Future = None):
        exception = None
        try:
            ret = await coro
        except Exception as e:
            exception = e
            pass
        finally:
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
        if exception is not None:
            if callback_future is None:
                raise exception
            else:
                callback_future.set_exception(exception)
                return
        if callback_future is None:
            return ret
        else:
            callback_future.set_result(ret)
            return

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

        if self._current_workers < self._max_workers:
            self._current_workers += 1
            coro = self._launcher(coroutine_function(*args, **kwargs))
            return asyncio.ensure_future(coro, loop=self._loop)
        else:
            future = asyncio.Future(loop=self._loop)
            coro = self._launcher(coroutine_function(*args, **kwargs),callback_future=future)

            def _single_generator(coro):
                yield coro
            self._pending_queue.append(_single_generator(coro))
            return future

    def _idle(self):
        return self._current_workers == 0 and len(self._pending_queue) == 0

    @overrides
    def map(self, coroutine_function, *iterables, limit=None, timeout=None):
        if coroutine_function is None or not asyncio.iscoroutinefunction(coroutine_function):
            raise TypeError("Must provide a coroutine function")
        if self._shutdowned:
            raise RuntimeError("Executor is closed.")

        queue = _async_generator(self._launcher, coroutine_function, iterables,
                                      self._loop, timeout=timeout, limit=limit)
        generator = queue.get_coroutine_generator()

        while self._current_workers < self._max_workers:
            try:
                current_coro = next(generator)
                self._current_workers += 1
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
                        try:
                            await future
                        except asyncio.CancelledError:
                            pass
                    self._futures_to_wait.clear()
            except asyncio.CancelledError:
                pass

        if wait:
            await _cancel_futures()
        else:
            return asyncio.ensure_future(_cancel_futures(), loop=self._loop)


class _async_generator():
    class AsyncResult(enum.Enum):
        result = 1
        exception = 2
        eof = 3

    def get_coroutine_generator(self):
        return self._generator

    def time_to_wait(self):
        if self._end_time is not None:
            current_wait = self._end_time - time.time()
            if current_wait < 0:
                current_wait = 0
            return current_wait
        return None

    def __init__(self, wrapper, coro_fun, iterables, loop, timeout=None, limit=None):
        self._wrapper = wrapper
        self._loop = loop
        self._end_time = None
        if timeout is not None:
            self._end_time = time.time() + timeout
        if limit is not None and limit > 0:
            self._sema = asyncio.BoundedSemaphore(value=limit, loop=self._loop)
        else:
            self._sema = None
        self._coro_fun = coro_fun

        self._running_futures = set()
        self._last_future = asyncio.Future(loop=self._loop)
        self._running_futures.add(self._last_future)
        self._async_result_queue = []
        self._wakeup_iterator = asyncio.Condition(loop=self._loop)
        self._iterables = iterables
        self._stopped = False
        self._generator = self._coroutine_generator()


    def _coroutine_generator(self):
        async def _wrapper(arg, loop):
            async def _inner_wrapper(arg, loop):
                try:
                    error = None
                    ret = None
                    try:
                        ret = await self._coro_fun(*arg, loop=loop)
                    except asyncio.CancelledError as e:
                        raise e
                    except Exception as e:
                        error = e
                    if self._sema is not None:
                        await self._sema.acquire()
                    async with self._wakeup_iterator:
                        if error is not None:
                            self._async_result_queue.append((self.AsyncResult.exception, error))
                        else:
                            self._async_result_queue.append((self.AsyncResult.result, ret))
                        self._wakeup_iterator.notify_all()
                except asyncio.CancelledError:
                    pass

            future = asyncio.ensure_future(_inner_wrapper(arg, loop), loop=loop)
            self._running_futures.add(future)
            try:
                await future
            finally:
                self._running_futures.remove(future)
                if len(self._running_futures) == 0:
                    try:
                        async with self._wakeup_iterator:
                            self._async_result_queue.append((self.AsyncResult.eof, None))
                            self._wakeup_iterator.notify_all()
                    except asyncio.CancelledError:
                        assert False
                        pass

        async def _terminator():
            async with self._wakeup_iterator:
                self._async_result_queue.append((self.AsyncResult.eof, None))
                self._wakeup_iterator.notify_all()

        def check(self):
            if self._last_future is not None:
                self._last_future.cancel()
                self._running_futures.remove(self._last_future)
                self._last_future = None
                if len(self._running_futures) == 0:
                    asyncio.ensure_future(_terminator(), loop=self._loop)
        for arg in zip(*self._iterables):
            if self._stopped:
                check(self)
                raise StopIteration
            try:
                yield self._wrapper(_wrapper(arg, self._loop))
            except GeneratorExit:
                check(self)
                raise StopIteration
        check(self)

    async def __anext__(self):
        if self._stopped:
            raise StopAsyncIteration
        async with self._wakeup_iterator:
            pending_timer = None
            while len(self._async_result_queue) == 0:
                wait_time = self.time_to_wait()
                if wait_time is not None:
                    async def _timer():
                        try:
                            await asyncio.sleep(wait_time, loop=self._loop)
                            async with self._wakeup_iterator:
                                self._wakeup_iterator.notify_all()
                        except asyncio.CancelledError:
                            pass
                    pending_timer = asyncio.ensure_future(_timer(), loop=self._loop)
                await self._wakeup_iterator.wait()
                if pending_timer is not None:
                    time_remaining = pending_timer.cancel()
                    await pending_timer
                    pending_timer = None
                    if not time_remaining:
                        self._stopped = True
                        self._generator.close()
                        current_wait = []
                        for future in self._running_futures:
                            future.cancel()
                            current_wait.append(future)
                        for future in current_wait:
                            await future
                        raise asyncio.TimeoutError("map timeout")
            (result_type, result) = self._async_result_queue.pop(0)
            if self._sema is not None:
                self._sema.release()
            if result_type == self.AsyncResult.eof:
                self._stopped = True
                raise StopAsyncIteration
            elif result_type == self.AsyncResult.exception:
                raise result
            elif result_type == self.AsyncResult.result:
                return result

    async def __aiter__(self):
        return self


import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
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
        async with CoroutinePoolExecutor(loop=loop, max_workers=1) as coex:
            try:
                async for ret in coex.map(test, range(10), range(10,20), timeout=2):
                    print(ret)
            except asyncio.TimeoutError:
                pass

    try:
        loop.run_until_complete(async_main(loop=loop))
    finally:
        loop.close()

    with ThreadPoolExecutor(max_workers=1) as exe:
        try:
            for x in exe.map(test_thread, range(10), range(10,20), timeout=2):
                print(x)
        except TimeoutError:
            pass

if __name__ == "__main__":
    main()
