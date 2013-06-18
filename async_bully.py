__author__ = 'terence'

import logging
import zerorpc
import gevent
import sys

STATE_NORM = 1
STATE_ELEC1 = 2
STATE_ELEC2 = 3
STATE_WAIT = 4

class StateVector():
    def __init__(self):
        # state of the node
        # [Norm, Elec1, Elec2, Wait]
        self.status = STATE_NORM
        # coordinator of the node
        self.ldr = -1
        #
        self.elid = -1
        # description of task
        self.down = None
        # the node recently makes this node halt
        self.acks = []
        # list of nodes which this node believes to be in operation
        self.nextel = -1
        #
        self.pendack = None


class AsyncBully():
    def __init__(self, addr, config_file='server_config'):
        self.S = StateVector()
        self.S.s = STATE_NORM

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

        #list of servers failure detectors are monitoring
        self.fd_list = range(len(self.servers))
        #stable storage
        self.incarn = 0

    def start_stage1(self):
        self.S.status = STATE_ELEC1
        self.S.elid = (self.i, self.incarn, self.S.nextel)
        self.S.nextel += 1
        self.S.down = []
        if self.i == 1:
            self.start_stage2()
        else:
            for i in xrange(self.i):
                self.start_fd(i)

    def halt(self, j, t):
        self.S.down.remove(j)
        self.start_fd(j)
        self.S.elid = t
        self.S.status = STATE_WAIT
        self.connections[j].accepted(self.i, t)

    def are_you_there(self):
        return True


    def start_stage2(self):
        for i in xrange(self.i, len(self.servers)):
            self.play_alive(i)
        self.S.elid = (self.i, self.incarn, self.S.nextel)
        self.S.nextel += 1
        self.S.status = 'Elect2'
        self.S.acks = []
        self.S.pendack = self.i
        self.contin_stage2()

    def contin_stage2(self):
        #TODO N
        if self.S.pendack < N:
            self.S.pendack += 1
            self.start_fd(self.S.pendack)
            self.connections[self.S.pendack].halt(self.i, self.S.elid)
        else: # I'm the leader
            self.S.ldr = self.i
            self.S.status = STATE_NORM
            for node in self.S.acks:
                #TODO: What is t?
                self.connections[node].set_leader(t)

    # On (Ack, t) from j
    def accepted(self, j, t):
        if self.S.status == STATE_ELEC2 and t == self.S.elid and j == self.S.pendack:
            self.S.acks.append(j)
            self.contin_stage2()

    # On (Ldr, t) from j
    def set_leader(self, j, t):
        if self.S.status == STATE_WAIT and t == self.S.elid:
            self.S.ldr = j
            self.S.status = STATE_NORM
            for i in xrange(len(self.servers)):
                if i != self.i:
                    self.stop_fd(i)
            self.start_fd(self.S.ldr)

    def are_you_normal(self, j):
        #TODO: pi(elid)
        if (self.S.s != STATE_NORM and j < pi(elid)) or \
                (self.S.status == STATE_NORM and j < self.S.ldr):
            return False
        else:
            return True

    def recovery(self):
        self.S.incarn += 1
        self.start_stage2()

    def check(self):
        while True:
            gevent.sleep(2)
            if self.S.status == STATE_NORM and self.S.ldr == self.i:
                for i, server in enumerate(self.servers[self.i + 1:]):
                    try:
                        normal = self.connections[i].are_you_normal(self.i)
                        print '%s : are_you_normal = %s' % (server, ans)
                        if normal:
                            if self.S.status == STATE_NORM and self.S.ldr == self.i and t == self.S.elid:
                                self.start_stage2()
                        else:
                            if self.S.status == STATE_NORM and self.S.ldr == self.i and t == self.S.elid:
                                self.start_stage2()

                    except zerorpc.TimeoutExpired:
                        print '%s Timeout!' % server
                        self.on_down_sig(i)
                        continue

            #TODO: failure detector?
            # elif self.S.s == STATE_NORM and self.S.c != self.i:
            #     logging.debug('check coordinator\'s state')
            #     try:
            #         result = self.connections[self.S.c].are_you_there()
            #         print '%s : are_you_there = %s' % (self.servers[self.S.c], result)
            #     except zerorpc.TimeoutExpired:
            #         print 'coordinator down, rasie eleciton.'
            #         self.on_down_sig(self.S.c)

    def on_down_sig(self, j):
        if (self.S.status == STATE_NORM and j == self.S.ldr) or \
                (self.S.status == STATE_WAIT and j == Pi(self.S.elid)):
            self.start_stage2()
        else:
            self.contin_stage2()

    def on_halt(self, j, t):
        if (self.S.status == STATE_NORM and self.S.ldr < j) or \
                (self.S.status == STATE_WAIT and Pi(self.S.elid) < j):
            self.connections[j].on_rej(t, self.i)
        else:
            self.halt(j, t)

    def on_rej(self, j, t):
        if self.S.status == STATE_ELEC2 and j == self.S.pendack:
            self.contin_stage2()

    def start(self):
        self.pool = gevent.pool.Group()
        self.recovery_greenlet = self.pool.spawn(self.recovery)

    def start_fd(self, j):
        if j not in self.fd_list:
            self.fd_list.append(j)


    def stop_fd(self, j):
        if j in self.fd_list:
            self.fd_list.remove(j)

    def play_alive(self, j):

    def play_dead(self, j):


    def main():
        logging.basicConfig(level=logging.DEBUG)
        addr = sys.argv[1]
        bully = AsyncBully(addr)
        s = zerorpc.Server(bully)
        s.bind('tcp://' + addr)
        bully.start()
        # Start server
        logging.debug('[%s] Starting ZeroRPC Server' % addr)
        s.run()


    if __name__ == '__main__':
        main()
