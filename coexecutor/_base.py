import asyncio
import abc

#3rdparty
try:
    from overrides import overrides
except:
    def overrides(function): #Annotation only
        return function
try:
    from overrides import final
except:
    def final(function):  # Annotation only
        return function


class CoExecutor:

    @abc.abstractmethod
    def submit(self, coroutine_function, *args, **kwargs) -> asyncio.Future:
        raise NotImplementedError()

    @abc.abstractmethod
    def map(self, coroutine_function, *iterables, timeout=None):
        raise NotImplementedError()

    @abc.abstractmethod
    async def shutdown(self, wait=True):
        raise NotImplementedError()

    @final
    async def __aenter__ (self):
        return self

    @final
    async def __aexit__(self, exc_type, exc, tb):
        await self.shutdown(wait=True)

    @final
    def __exit__(self, exc_type, exc_val, exc_tb):
        raise NotImplementedError("Use CoExecutor with \"async with\" statement")

    @final
    def __enter__(self):
        raise NotImplementedError("Use CoExecutor with \"async with\" statement")
