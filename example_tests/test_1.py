import random
import time


def test1():
    time.sleep(1 + random.random())


def test2():
    time.sleep(1 + random.random())
    assert False


def test3():
    time.sleep(1 + random.random())


def test4():
    time.sleep(1 + random.random())
