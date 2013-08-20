import _pytest.runner
import marshal
import os
import py
import pytest
import Queue
import sys
import time


def pytest_addoption(parser):
    group = parser.getgroup('robust', 'Robust subprocess testing')
    group.addoption('--robust', action='store_true', default=False,
                    help='enable robust mode')
    group.addoption('--robust-batch', action='store', default=1,
                    metavar='1', type=int,
                    help='each subprocess will execute at most this many tests')
    group.addoption('--robust-master', action='store', default=None,
                    metavar='URL', type=str,
                    help='for internal usage')


def pytest_addhooks(pluginmanager):
    return  # TODO: skip for now
    import pytest_robust.newhooks
    pluginmanager.addhooks(pytest_robust.newhooks)


def pytest_collection(session):
    pass
    # TODO: Skip collection in workers.
    # if session.config.option.robust_master:
    #     return True


# TODO:
# item_worker_map = {}
workers = []


def pytest_runtestloop(session):
    if not session.config.getvalue('robust'):
        return False
    if session.config.option.collectonly:
        return True

    if session.config.option.robust_master:
        worker(session)
        return True

    item_queue = Queue.Queue()
    report_queue = Queue.Queue()

    for item in session.items:
        item_queue.put(serialize_test(item))

    server_socket, server_active = start_server(item_queue, report_queue)
    master_url = 'tcp://%s:%d' % server_socket.getsockname()

    # TODO: spawn thread to watch CPU load and fork new runners
    for _ in range(8):
        spawn_worker(session, master_url)

    while not item_queue.empty():
        if session.shouldstop:
            raise session.Interrupted(session.shouldstop)

        time.sleep(0.1)

    while True:
        if session.shouldstop:
            raise session.Interrupted(session.shouldstop)

        report = None
        try:
            report = report_queue.get(False)
        except Queue.Empty:
            pass
        if report is not None:
            item_d, report_d = report
            item = find_test_item(session, item_d)
            report = deserialize_report(report_d)
            session.config.hook.pytest_runtest_logreport(report=report)

        done = 0
        for p in workers:
            if p.poll() is not None:
                if p.returncode != 0:
                    # TODO: handle worker crash
                    '''
                    path, lineno = item._getfslineno()
                    message = '%s:%s:\ntest CRASHED signal: %d error:\n%s' % (
                        path, lineno, result.signal, result.err)

                    reports = [make_report(item, message)]
                    '''
                    pass

                done += 1
        if done == len(workers):
            break

    return True


def find_test_item(session, d):
    item = None
    for it in session.items:
        if str(it.fspath) == d['fspath'] and it.name == d['name']:
            item = it
            break
    return item


def make_report(item, message):
    call = _pytest.runner.CallInfo(lambda: 0 / 0, '???')
    call.excinfo = message
    report = _pytest.runner.pytest_runtest_makereport(item, call)
    return report


# master communication with workers

import _pytest.main
import marshal
import os
import socket
import subprocess
import threading


# Message format:
# "ROBUST1:" message length ":" marshal.dumps([command, args])
# message length is encoded as '%07d'
# decimal number with leading zeros, up to 7 bytes
# So fixed header length is 16 bytes, then <length> bytes of marshalled data.


def read_message(f):
    header = f.read(16)
    if header == '':
        raise EOFError()
    assert len(header) == 16
    assert header[:8] == 'ROBUST1:'
    assert header[15] == ':'
    length = int(header[8:15])
    body = f.read(length)
    assert len(body) == length
    message = marshal.loads(body)
    return message


def encode_message(msg):
    body = marshal.dumps(msg)
    s = 'ROBUST1:%07d:%s' % (len(body), body)
    return s


def start_server(item_queue, report_queue):
    server_sock = socket.socket()
    server_sock.bind(('localhost', 0))
    server_sock.listen(5)
    active = [True]

    def listener():
        while active[0]:
            client = server_sock.accept()
            t = threading.Thread(target=talker, args=client)
            t.daemon = True
            t.start()

    def talker(sock, addr):
        # print 'worker connected', addr
        sock.settimeout(10)
        f = sock.makefile()
        # FIXME
        # It's more convenient to use sock.makefile() for buffering here.
        # But docs explicitly say that
        # "The socket must be in blocking mode (it can not have a timeout)."
        # http://docs.python.org/2/library/socket.html#socket.socket.makefile
        # Timeouts are absolutely required.
        while active[0]:
            try:
                msg = read_message(f)
                # print 'master received message:', msg

                if msg[0] == 'acquire':
                    items = []
                    try:
                        items.append(item_queue.get(False))
                    except Queue.Empty:
                        pass
                    # print 'master sending tests:', repr(items)
                    f.write(encode_message(['tests', items]))
                    f.flush()
                elif msg[0] == 'report':
                    for r in msg[1]:
                        report_queue.put(r)
                else:
                    raise Exception(
                        'unknown message from worker: ' + repr(msg))

            except EOFError:
                f.close()
                sock.close()
                break

            except Exception:
                f.close()
                sock.close()
                raise

    t = threading.Thread(target=listener)
    t.daemon = True
    t.start()

    return (server_sock, active)


def spawn_worker(session, master_url):
    argv = [sys.executable] + sys.argv + ['--robust-master', master_url]

    # Note that on Windows, you cannot set close_fds to true and also
    # redirect the standard handles by setting stdin, stdout or stderr.
    # http://docs.python.org/2/library/subprocess.html
    close_fds = os.name != 'nt'

    p = subprocess.Popen(
        argv,
        bufsize=32 << 10,
        close_fds=close_fds,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    workers.append(p)
    return p


def serialize_test(item):
    d = {
        'name': item.name,
        'fspath': str(item.fspath),
    }
    return d


def serialize_report(report):
    d = report.__dict__.copy()
    if hasattr(report.longrepr, 'toterminal'):
        d['longrepr'] = str(report.longrepr)
    else:
        d['longrepr'] = report.longrepr
    for name in d:
        if isinstance(d[name], py.path.local):
            d[name] = str(d[name])
        elif name == "result":
            d[name] = None  # for now
    return d


def deserialize_report(d):
    return _pytest.runner.TestReport(**d)


# worker process
import _pytest.runner
import marshal
import socket
import time


def master_connect(url):
    assert url.startswith('tcp://')
    parts = url[6:].split(':', 1)
    assert len(parts) == 2
    host = parts[0]
    port = int(parts[1])

    sock = socket.socket()
    sock.settimeout(10)
    sock.connect((host, port))

    return sock


def master_acquire(f):
    msg = ['acquire']
    s = encode_message(msg)
    f.write(s)
    f.flush()
    response = read_message(f)
    assert response[0] == 'tests'
    return response[1]


def master_report(f, reports):
    msg = ['report', reports]
    s = encode_message(msg)
    f.write(s)
    f.flush()


def worker(session):
    active = [True]
    master_sock = master_connect(session.config.option.robust_master)
    master_f = master_sock.makefile()

    while active[0]:
        items = master_acquire(master_f)
        if not items:
            return
        # print 'worker acquired tests:', items

        for d in items:
            # TODO: deserialize items from master, remove collection in workers
            item = find_test_item(session, d)
            if item is None:
                print 'worker acquired unknown test:', d
                return

            reports = _pytest.runner.runtestprotocol(
                item, nextitem=None, log=False)
            reports = [
                (d, serialize_report(r))
                for r in reports
            ]

            master_report(master_f, reports)
