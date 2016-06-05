import time
import asyncio
import random
import threading
import functools
from coexecutor import CoroutinePoolExecutor
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor

_max_time = 5
_precision = 0.1

time_limit = round(_max_time / _precision)


def wake_at(at):
    current = time.time()
    assert at > current, "Time skewed"
    time.sleep(at - current)
    current = time.time()
    diff = abs(at-current)
    assert diff < _precision, "Timer accuracy failed: %s" % diff
    return at

global_result = [[]]

async def async_wake_at(at,*,loop):
    current = time.time()
    assert at > current, "Time skewed: %s" % (at-current)
    await asyncio.sleep(at - current, loop=loop)
    current = time.time()
    diff = abs(at - current)
    assert diff < _precision, "Timer accuracy failed: %s" % diff
    global_result[0].append(at)
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
            r = random.randint(1, num_workers * 3 + 1)
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
    input = input_generator(workers)
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

    result = True
    for (t,p,c) in zip(tresult,presult,cresult):
        result = result and (t == p)
        if not result:
            assert False
        result = result and (p == c)
        if not result:
            assert False
        result = result and (c == t)
        if not result:
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
    print(pre_input1)
    pre_input2 = input_generator(workers, max(pre_input1))
    print(pre_input2)

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

    # for x in input1:
    #     future = texec.submit(wake_at, x)
    #     future.add_done_callback(
    #         functools.partial(result_checker, tresult, lock))
    for x in texec.map(wake_at, iter(input1)):
        with lock:
            tresult.append(x)

    texec.shutdown(True)

    pstart = round(time.time() + 1)
    input1 = [pstart + i for i in pre_input1]
    input2 = [pstart + i for i in pre_input2]

    # for x in input1:
    #     future = pexec.submit(wake_at, x)
    #     future.add_done_callback(
    #         functools.partial(result_checker, presult, lock))
    for x in pexec.map(wake_at, iter(input1)):
        with lock:
            presult.append(x)

    pexec.shutdown(True)

    cstart = round(time.time() + 1)
    input1 = [cstart + i for i in pre_input1]
    input2 = [cstart + i for i in pre_input2]

    async def async_main():
        # for x in input1:
        #     future = cexec.submit(async_wake_at, x)
        #     future.add_done_callback(
        #         functools.partial(result_checker, cresult, lock))
        async for x in cexec.map(async_wake_at, iter(input1)):
            with lock:
                cresult.append(x)
        await cexec.shutdown(False)

    loop.run_until_complete(async_main())

    try:
        loop.run_until_complete(cexec.shutdown(True))
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
    cresult = []
    print("Current worker", workers)
    pre_input1 = input_generator(workers, 0)
    pre_input2 = input_generator(workers, max(pre_input1))

    def result_checker(list, lock, fut):
        with lock:
            try:
                list.append(fut.result())
            except Exception as e:
                list.append(e)

    cexec = CoroutinePoolExecutor(**param, loop=loop, debug=True)

    global_result[0] = []
    cstart = round(time.time() + 1)
    input1 = [cstart + i for i in pre_input1]
    input2 = [cstart + i for i in pre_input2]

    async def async_main():
        for x in input1:
            future = cexec.submit(async_wake_at, x)
            future.add_done_callback(
                functools.partial(result_checker, cresult, lock))
        async for x in cexec.map(async_wake_at, input2):
            with lock:
                cresult.append(x)
        await cexec.shutdown(False)

    loop.run_until_complete(async_main())

    try:
        loop.run_until_complete(cexec.shutdown(True))
    finally:
        loop.close()
    cresult = list([round(i-cstart,1) for i in cresult])
    global_result[0] = list([round(i - cstart,1) for i in global_result[0]])
    total = list([round(x,1) for x in pre_input1 + pre_input2])
    print("cresult", cresult)
    print("gresult", global_result[0])
    print("total", total)
    total.sort()
    print("total_sort", total)
    print(cresult == global_result[0])
    global_result[0].sort()
    print(cresult == global_result[0])

    print(cresult == total)
    return True


if __name__ == '__main__':
    do_test2(2)
    exit(0)
    do_test3(1)
    do_test3(2)
    do_test3(3)
    do_test3(4)
    do_test3(5)
    do_test3(6)
    do_test3(7)
    #unittest.main()
