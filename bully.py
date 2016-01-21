__author__ = 'terence'

import zerorpc
import gevent
import sys


class StateVector():
    def __init__(self):
        # state of the node
        # [Down, Election, Reorganization, Normal]
        self.s = 'Normal'
        # coordinator of the node
        self.c = 0
        # description of task
        self.d = None
        # the node recently makes this node halt
        self.h = -1
        # list of nodes which this node believes to be in operation
        self.Up = []


class Bully():
    def __init__(self, addr, config_file='server_config'):
        self.S = StateVector()
        self.S.s = 'Normal'

        self.check_servers_greenlet = None

        self.addr = addr

        self.servers = []
        f = open(config_file, 'r')
        for line in f.readlines():
            line = line.rstrip()
            self.servers.append(line)
        print 'My addr: %s' % self.addr
        print 'Server list: %s' % (str(self.servers))

        self.n = len(self.servers)

        self.connections = []

        for i, server in enumerate(self.servers):
            if server == self.addr:
                self.i = i
                self.connections.append(self)
            else:
                c = zerorpc.Client(timeout=2)
                c.connect('tcp://' + server)
                self.connections.append(c)

    def are_you_there(self):
        return True

    def are_you_normal(self):
        if self.S.s == 'Normal':
            return True
        else:
            return False

    def halt(self, j):
        self.S.s = 'Election'
        self.S.h = j

    def new_coordinator(self, j):
        print 'call new_coordinator'
        if self.S.h == j and self.S.s == 'Election':
            self.S.c = j
            self.S.s = 'Reorganization'

    def ready(self, j, x=None):
        print 'call ready'
        if self.S.c == j and self.S.s == "Reorganization":
            self.S.d = x
            self.S.s = 'Normal'

    def election(self):
        print 'Check the states of higher priority nodes:'

        for i, server in enumerate(self.servers[self.i + 1:]):
            try:
                self.connections[self.i + 1 + i].are_you_there()
                if self.check_servers_greenlet is None:
                    self.S.c = self.i + 1 + i
                    self.S.s = 'Normal'
                    self.check_servers_greenlet = self.pool.spawn(self.check())
                return
            except zerorpc.TimeoutExpired:
                print '%s Timeout!' % server

        print 'halt all lower priority nodes including this node:'
        self.halt(self.i)
        self.S.s = 'Election'
        self.S.h = self.i
        self.S.Up = []
        for i, server in enumerate(self.servers[self.i::-1]):
            try:
                self.connections[i].halt(self.i)
            except zerorpc.TimeoutExpired:
                print '%s Timeout!' % server
                continue
            self.S.Up.append(self.connections[i])

        # reached 'election point',inform nodes of new coordinator
        print 'inform nodes of new coordinator:'
        self.S.c = self.i
        self.S.s = 'Reorganization'
        for j in self.S.Up:
            try:
                j.new_coordinator(self.i)
            except zerorpc.TimeoutExpired:
                print 'Timeout! Election will be restarted.'
                self.election()
                return

        # Reorganization
        for j in self.S.Up:
            try:
                j.ready(self.i, self.S.d)
            except zerorpc.TimeoutExpired:
                print 'Timeout!'
                self.election()
                return

        self.S.s = 'Normal'
        print '[%s] Starting ZeroRPC Server' % self.servers[self.i]
        self.check_servers_greenlet = self.pool.spawn(self.check())

    def recovery(self):
        self.S.h = -1
        self.election()

    def check(self):
        while True:
            gevent.sleep(2)
            if self.S.s == 'Normal' and self.S.c == self.i:
                for i, server in enumerate(self.servers):
                    if i != self.i:
                        try:
                            ans = self.connections[i].are_you_normal()
                            print '%s : are_you_normal = %s' % (server, ans)
                        except zerorpc.TimeoutExpired:
                            print '%s Timeout!' % server
                            continue

                        if not ans:
                            self.election()
                            return
            elif self.S.s == 'Normal' and self.S.c != self.i:
                print 'check coordinator\'s state'
                try:
                    result = self.connections[self.S.c].are_you_there()
                    print '%s : are_you_there = %s' % (self.servers[self.S.c], result)
                except zerorpc.TimeoutExpired:
                    print 'coordinator down, rasie eleciton.'
                    self.timeout()

    def timeout(self):
        if self.S.s == 'Normal' or self.S.s == 'Reorganization':
            try:
                self.connections[self.S.c].are_you_there()
            except zerorpc.TimeoutExpired:
                print '%s Timeout!' % self.servers[self.S.c]
                self.election()
        else:
            self.election()

    def start(self):
        self.pool = gevent.pool.Group()
        self.recovery_greenlet = self.pool.spawn(self.recovery)


def main():
    addr = sys.argv[1]
    bully = Bully(addr)
    s = zerorpc.Server(bully)
    s.bind('tcp://' + addr)
    bully.start()
    # Start server
    print '[%s] Starting ZeroRPC Server' % addr
    s.run()


if __name__ == '__main__':
    main()
