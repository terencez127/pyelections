__author__ = 'terence'

import logging
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
        self.up = []
        # group
        self.g = -1
        #counter
        self.counter = 0


class Invitation():
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

    def check(self):
        while True:
            gevent.sleep(2)
            if self.S.s == 'Normal' and self.S.c == self.i:
                temp_set = []
                for i, server in enumerate(self.servers):
                    if i != self.i:
                        try:
                            ans = self.connections[i].are_you_coordinator()
                            print '%s : are_you_coordinator = %s' % (server, ans)
                        except zerorpc.TimeoutExpired:
                            print '%s Timeout!' % server
                            continue

                        if ans:
                            temp_set.append(i)

                if len(temp_set) == 0:
                    return

                p = sorted(temp_set)[-1]

                if self.i < p:
                    #TODO: wait time proportional to p - i

                self.merge()


            elif self.S.s == 'Normal' and self.S.c != self.i:
                logging.debug('check coordinator\'s state')
                try:
                    result = self.connections[self.S.c].are_you_there()
                    print '%s : are_you_there = %s' % (self.servers[self.S.c], result)
                except zerorpc.TimeoutExpired:
                    print 'coordinator down, rasie eleciton.'
                    self.timeout()

    def timeout(self):
        my_coord = self.S.c
        my_group = self.S.g
        if my_coord == self.i:
            return
        else:
            try:
                result = self.connections[my_coord].are_you_there(my_group, self.i)
            except zerorpc.TimeoutExpired:
                self.recovery()

            if not result:
                self.recovery()

    def merge(self, coord_set):
        self.S.s = 'Election'

        #TODO: call stop

        self.S.counter += 1
        self.S.g = int(str(self.i) + str(self.S.counter))
        self.S.c = self.i
        temp_set = self.S.up
        self.S.up = []

        for j in coord_set:
            try:
                self.connections[j].invitation(self.i, self.S.g)
            except zerorpc.TimeoutExpired:
                continue

        for j in temp_set:
            try:
                self.connections[j].invitation(self.i, self.S.g)
            except zerorpc.TimeoutExpired:
                continue

        #TODO:  wait a reasonable time for accept messages to come in

        self.S.s = 'Reorganization'
        for j in self.S.up:
            try:
                self.connections[j].ready(self.i, self.S.g, self.S.d)
            except zerorpc.TimeoutExpired:
                self.recovery()

        self.S.s = 'Normal'

    def ready(self, j, gn, x=None):
        logging.debug('call ready')
        if self.S.s == "Reorganization" and self.S.g == gn:
            self.S.d = x
            self.S.s = 'Normal'

    def are_you_coordinator(self):
        if self.S.s == 'Normal' and self.S.c == self.i:
            return True
        else:
            return False

    def are_you_there(self, gn, j):
        if self.S.g == gn and self.S.c == self.i and j in self.S.up:
            return True
        else:
            return False

    def invitation(self, j, gn):
        if self.S.s != 'Normal':
            return
        temp = self.S.c
        temp_set = self.S.up
        self.S.s = 'Election'
        self.S.c = j
        self.S.g = gn
        if temp == self.i:
            for k in temp_set:
                try:
                    self.connections[j].invitation(k, j, gn)
                except zerorpc.TimeoutExpired:
                    print 'Timeout!'
        try:
            self.connections[j].Accept(self.i, gn)
        except zerorpc.TimeoutExpired:
            self.recovery()

        #TODO: can't execute if recovery() is called
        self.S.s = 'Reorganization'

    def accept(self, j, gn):
        if self.S.s == 'Election' and self.S.g == gn and self.S.c == self.i:
            self.S.up.append(j)

    def recovery(self):
        self.S.s = 'Election'
        self.counter += 1
        self.S.g = int(str(self.i) + str(self.S.counter))
        self.S.c = self.i
        self.S.up = []
        self.S.s = 'Reorganization'
        self.S.s = 'Normal'
        self.check_servers_greenlet = self.pool.spawn(self.check)

    def start(self):
        self.pool = gevent.pool.Group()
        self.recovery_greenlet = self.pool.spawn(self.recovery)


    def main():
        logging.basicConfig(level=logging.DEBUG)
        addr = sys.argv[1]
        bully = Invitation(addr)
        s = zerorpc.Server(bully)
        s.bind('tcp://' + addr)
        bully.start()
        # Start server
        logging.debug('[%s] Starting ZeroRPC Server' % addr)
        s.run()


    if __name__ == '__main__':
        main()