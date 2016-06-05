import time
import asyncio
import random
import threading
import functools
from coexecutor import CoroutinePoolExecutor
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor

_max_time = 3
_precision = 0.1

time_limit = round(_max_time / _precision)


def wake_at(at):
    current = time.time()
    assert at > current, "Time skewed: %s" % (at - current)
    time.sleep(at - current)
    current = time.time()
    diff = abs(at-current)
    assert diff < _precision, "Timer accuracy failed: %s" % diff
    return at

async def async_wake_at(at,*,loop):
    current = time.time()
    assert at > current, "Time skewed: %s" % (at-current)
    await asyncio.sleep(at - current, loop=loop)
    current = time.time()
    diff = abs(at - current)
    assert diff < _precision, "Timer accuracy failed: %s" % diff
    return at


def input_generator(num_workers, start_time=None):
    if start_time is None:
        start_time = time.time() + 1
    input = []
    current_base = 0

    pre_input = []
    while current_base < time_limit:
        unique = set()
        while len(unique) != num_workers:
            r = random.randint(0, num_workers*3)+1
            unique.add(r)
        sample = list(unique)
        random.shuffle(sample)
        for x in sample:
            pre_input.append(current_base + x)
        current_base += max(unique)

    start_time = round(start_time)

    for x in pre_input:
        if x > time_limit:
            break
        input.append(start_time + (x * _precision))
    return input


def do_test1(workers):
    param = {"max_workers": workers}
    start = round(time.time() + 1)
    input = input_generator(workers, start)
    loop = asyncio.new_event_loop()

    lock = threading.Lock()
    tresult = []
    presult = []
    cresult = []

    def result_checker(list, lock, fut):
        with lock:
            try:
                list.append(fut.result())
            except Exception as e:
                list.append(e)

    texec = ThreadPoolExecutor(**param)
    pexec = ProcessPoolExecutor(**param)
    cexec = CoroutinePoolExecutor(**param, loop=loop)

    for x in input:
        future = texec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, tresult, lock))

        future = pexec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, presult, lock))

        future = cexec.submit(async_wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, cresult, lock))

    texec.shutdown(False)
    pexec.shutdown(False)
    loop.run_until_complete(cexec.shutdown(False))

    try:
        loop.run_until_complete(cexec.shutdown(True))
        texec.shutdown(True)
        pexec.shutdown(True)
    finally:
        loop.close()

    tresult = [round((x - start) / _precision) for x in tresult]
    presult = [round((x - start) / _precision) for x in presult]
    cresult = [round((x - start) / _precision) for x in cresult]

    result = True
    for (t, p, c) in zip(tresult, presult, cresult):
        result = result and (t == p)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
        result = result and (p == c)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
        result = result and (c == t)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
    return result


def do_test2(workers):
    param = {"max_workers": workers}
    loop = asyncio.new_event_loop()

    lock = threading.Lock()
    tresult = []
    presult = []
    cresult = []

    pre_input1 = input_generator(workers, 0)

    def result_checker(list, lock, fut):
        with lock:
            try:
                list.append(fut.result())
            except Exception as e:
                list.append(e)

    texec = ThreadPoolExecutor(**param)
    pexec = ProcessPoolExecutor(**param)
    cexec = CoroutinePoolExecutor(**param, loop=loop)

    tstart = round(time.time()+1)
    input1 = [tstart + i for i in pre_input1]

    for x in texec.map(wake_at, input1):
        with lock:
            tresult.append(x)

    texec.shutdown(True)

    pstart = round(time.time() + 1)
    input1 = [pstart + i for i in pre_input1]

    for x in pexec.map(wake_at, input1):
        with lock:
            presult.append(x)

    pexec.shutdown(True)

    cstart = round(time.time() + 1)
    input1 = [cstart + i for i in pre_input1]

    async def async_main():
        async for x in cexec.map(async_wake_at, input1):
            with lock:
                cresult.append(x)
        await cexec.shutdown(False)

    loop.run_until_complete(async_main())

    try:
        loop.run_until_complete(cexec.shutdown(True))
        texec.shutdown(True)
        pexec.shutdown(True)
    finally:
        loop.close()

    tresult = [round((x - tstart) / _precision) for x in tresult]
    presult = [round((x - pstart) / _precision) for x in presult]
    cresult = [round((x - cstart) / _precision) for x in cresult]

    result = True
    for (t, p, c) in zip(tresult, presult, cresult):
        result = result and (t == p)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t,p,c)
            assert False
        result = result and (p == c)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
        result = result and (c == t)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
    return result


def do_test3(workers):
    param = {"max_workers": workers}
    loop = asyncio.new_event_loop()

    lock = threading.Lock()
    tresult = []
    presult = []
    cresult = []

    pre_input1 = input_generator(workers, 0)
    pre_input2 = input_generator(workers, max(pre_input1))
    pre_input3 = input_generator(workers, max(pre_input2))

    def result_checker(list, lock, fut):
        with lock:
            try:
                list.append(fut.result())
            except Exception as e:
                list.append(e)

    texec = ThreadPoolExecutor(**param)
    pexec = ProcessPoolExecutor(**param)
    cexec = CoroutinePoolExecutor(**param, loop=loop)

    tstart = round(time.time()+1)
    input1 = [tstart + i for i in pre_input1]
    input2 = [tstart + i for i in pre_input2]
    input3 = [tstart + i for i in pre_input3]

    for x in input1:
        future = texec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, tresult, lock))
    result_iter = texec.map(wake_at, input2)
    for x in input3:
        future = texec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, tresult, lock))
    for x in result_iter:
        with lock:
            tresult.append(x)

    texec.shutdown(True)

    pstart = round(time.time() + 1)
    input1 = [pstart + i for i in pre_input1]
    input2 = [pstart + i for i in pre_input2]
    input3 = [pstart + i for i in pre_input3]

    for x in input1:
        future = pexec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, presult, lock))
    result_iter = pexec.map(wake_at, input2)
    for x in input3:
        future = pexec.submit(wake_at, x)
        future.add_done_callback(
            functools.partial(result_checker, presult, lock))
    for x in result_iter:
        with lock:
            presult.append(x)

    pexec.shutdown(True)

    cstart = round(time.time() + 1)
    input1 = [cstart + i for i in pre_input1]
    input2 = [cstart + i for i in pre_input2]
    input3 = [cstart + i for i in pre_input3]

    async def async_main():
        for x in input1:
            future = cexec.submit(async_wake_at, x)
            future.add_done_callback(
                functools.partial(result_checker, cresult, lock))
        result_iter = cexec.map(async_wake_at, input2)
        for x in input3:
            future = cexec.submit(async_wake_at, x)
            future.add_done_callback(
                functools.partial(result_checker, cresult, lock))
        async for x in result_iter:
            with lock:
                cresult.append(x)
        await cexec.shutdown(False)

    loop.run_until_complete(async_main())

    try:
        loop.run_until_complete(cexec.shutdown(True))
        texec.shutdown(True)
        pexec.shutdown(True)
    finally:
        loop.close()

    tresult = [round((x - tstart) / _precision) for x in tresult]
    presult = [round((x - pstart) / _precision) for x in presult]
    cresult = [round((x - cstart) / _precision) for x in cresult]

    result = True
    for (t, p, c) in zip(tresult, presult, cresult):
        result = result and (t == p)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t,p,c)
            assert False
        result = result and (p == c)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
        result = result and (c == t)
        if not result:
            print(tresult)
            print(presult)
            print(cresult)
            print(t, p, c)
            assert False
    return result

