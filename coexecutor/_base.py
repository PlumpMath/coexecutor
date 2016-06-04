import asyncio

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

    def submit(self, coroutine_function, *args, **kwargs) -> asyncio.Future:
        raise NotImplementedError()

    def map(self, coroutine_function, *iterables, timeout=None):
        raise NotImplementedError()

    async def shutdown(self, wait=True):
        raise NotImplementedError()

    @final
    async def __aenter__ (self):
        return self

    @final
    async def __aexit__(self, exc_type, exc, tb):
        await self.shutdown(wait=True)
