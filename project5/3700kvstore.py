#!/usr/bin/env python

import sys, socket, select, time, json, random, math

# Your ID number
my_id = sys.argv[1]

# The ID numbers of all the other replicas
replica_ids = sys.argv[2:]

last = 0

class KVStore:
    
    def __init__(self, id): 
        self.id = id
        self.db = {}
        self.leader = 'FFFF'
        
        self.term = 0
        self.index = -1
        
        self.log = []
        
        self.last_heartbeat = time.time()
        self.leader_timeout = .3
        
        self.num_replicas = len(replica_ids) + 1
        
        self.state = 'follower'
        
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        self.sock.connect(self.id)
        
    def redirect_to_leader(self, msg):
        to_send = {'src': self.id, 'dst': msg['src'], 'MID': msg['MID'],  'type': 'redirect', 'leader': self.leader}
        self.sock.send(json.dumps(to_send))
                    
    def send_put_to_replicas(self, msg):
        to_send = {'src': self.id, 'dst': 'FFFF', 'type': msg['type'], 'leader': self.id, 'key': msg['key'], 'value': msg['value'], 'MID': msg['MID']}
        self.sock.send(json.dumps(to_send))
        
    def send_put_response(self, msg):
        to_send = {'src': self.id, 'dst': msg['src'], 'leader': self.leader, 'type': 'ok', 'MID': msg['MID']}
        self.sock.send(json.dumps(to_send))
        
    def send_get_response(self, msg, value):
        to_send = {'src': self.id, 'dst': msg['src'], 'leader': self.leader, 'type': 'ok', 'MID': msg['MID'], 'value':value}
        self.sock.send(json.dumps(to_send))
        
    def send_failed_response(self, msg):
        to_send = {'src': self.id, 'dst': msg['src'], 'leader': self.leader, 'type': 'fail', 'MID': msg['MID']}
        self.sock.send(json.dumps(msg))
        
    def send_request(self, index):
        to_send = {'src': self.id, 'dst': self.leader, 'leader': self.leader, 'type': 'request', 'index': index}
        self.sock.send(json.dumps(to_send))
        
    def send_RPC(self): 
        key = ""
        value = ""
        if self.index >= 0:
            key = self.log[self.index][0]
            value = self.log[self.index][1]
            
        msg = {'src': self.id, 'dst': 'FFFF', 'leader': self.leader, 'type': 'rpc', 'term': self.term, 'index': self.index, 'key': key, 'value': value}
        self.sock.send(json.dumps(msg))
        
    def send_specific_RPC(self, index, dst):
        key = self.log[index][0]
        value = self.log[index][1]
        msg = {'src': self.id, 'dst': dst, 'leader': self.leader, 'type': 'rpc', 'term': self.term, 'index': index, 'key': key, 'value': value}
        self.sock.send(json.dumps(msg))
        
        
    def send_vote(self, dest):
        msg = {'src': self.id, 'dst': dest, 'leader': dest, 'type': 'vote'}
        self.sock.send(json.dumps(msg))
        
    def begin_election(self):
        self.leader_timeout = 2
        #print self.id + ": is trying to be leader"
        self.leader = 'FFFF'
        self.state = 'candidate'
        self.send_RPC()
        time_of_last_rpc = time.time()
        votes = [self.id] 
        election_start = time.time()
        election_timeout = (random.random() * .4) + .3
        while len(votes) < math.ceil(self.num_replicas / 2.0):
            if time.time() - time_of_last_rpc > .15:
                self.send_RPC()
                time_of_last_rpc = time.time()
            
            if time.time() - election_start > election_timeout:
                #timed out, abort election
                self.state = 'follower'
                self.last_heartbeat = time.time()
                return
                
            ready = select.select([self.sock], [], [], 0.1)[0]
            if self.sock in ready:
                msg_raw = self.sock.recv(32768)
                
                if len(msg_raw) == 0:
                    continue
                    
                msg = json.loads(msg_raw)
                
                if msg['type'] in ['get', 'put']:
                    self.send_failed_response(msg)
                    
                elif msg['type'] == 'rpc':
                    #only one case to worry about
                    if msg['leader'] == msg['src']:
                        #this person has won an election
                        if msg['term'] > self.term:
                            #They're legit, go with them
                            self.leader = msg['src']
                            self.state = 'follower'
                            #print self.id + ": lost the election"
                            self.last_heartbeat = time.time()
                            return
                            
                elif msg['type'] == 'vote':
                    if msg['src'] not in votes:
                        votes.append(msg['src'])
                        
        #after winning the election
        self.leader = self.id
        self.term += 1
        self.state = 'leader'
        self.send_RPC()
        print self.id + " JUST WON THE ELECTION"
        #for vote in votes:
           # print vote
        
    def vote_in_election(self, candidate_id):
        self.leader_timeout = 2
        self.leader = 'FFFF'
        self.send_vote(candidate_id)
        #print self.id + ": voted for " + candidate_id
        last_voted = time.time()
        while True:
            if time.time() - last_voted > .25:
                self.send_vote(candidate_id)
        
            ready = select.select([self.sock], [], [], 0.1)[0]
            if self.sock in ready:
                msg_raw = self.sock.recv(32768)
                
                if len(msg_raw) == 0:
                    continue
                    
                msg = json.loads(msg_raw)
                
                if msg['type'] in ['get', 'put']:
                    self.send_failed_response(msg)
                    
                elif msg['type'] == 'rpc':
                
                    if msg['leader'] == msg['src']:
                        #someone won
                        self.last_heartbeat = time.time()
                        self.term += 1
                        break
                    elif msg['term'] > self.term:
                        #apparently a new election started
                        self.vote_in_election(msg['src'])
                        return
                    else:
                        continue
                        
        
        #after the election is over
        self.leader = msg['leader']
        
        
    def time_to_elect_new_leader(self):
        return time.time() - self.last_heartbeat > self.leader_timeout and not self.id == self.leader
    
    def get(self, key):
        if key in self.db:
            #print self.id + ": got (" + key + ", " + self.db[key] + ")"
            return self.db[key]
        else:
            return ""
        
    def put(self, key, value):
        self.db[key] = value
        
    def addToLog(self, key, value):
        self.log.append((key, value))
        self.index += 1
        
    def is_leader(self):
        return self.id == self.leader
        

kvstore = KVStore(my_id)
time.sleep(.5)
kvstore.last_heartbeat = time.time()
while True:
    if kvstore.time_to_elect_new_leader():
        #print kvstore.id + "starting election"
        kvstore.begin_election()
        continue
        
    ready = select.select([kvstore.sock], [], [], 0.1)[0]
    
    if kvstore.sock in ready:
        msg_raw = kvstore.sock.recv(32768)
        
        if len(msg_raw) == 0: 
            continue
            
        msg = json.loads(msg_raw)
        
        # For now, ignore get() and put() from clients
        if msg['type'] == 'get':
            if kvstore.is_leader():
                value = kvstore.get(msg['key'])
                kvstore.send_get_response(msg, value)
            else:
                kvstore.redirect_to_leader(msg)
        
        elif msg['type'] == 'put':
            if msg['src'] == kvstore.id:
                print "got message from self"
                pass
                
            if kvstore.is_leader():
                kvstore.addToLog(msg['key'], msg['value'])
                kvstore.put(msg['key'], msg['value'])
                
                kvstore.send_RPC()
                kvstore.send_put_response(msg)
            else:
                kvstore.redirect_to_leader(msg)
                
        
        elif msg['type'] == 'request':
            if kvstore.is_leader():
                kvstore.send_specific_RPC(msg['index'], msg['src'])
            
        elif msg['type'] == 'rpc':
            if msg['leader'] == msg['src']:
                #this person claims to have won an election
                if msg['leader'] != kvstore.leader:
                    if msg['term'] > kvstore.term:
                        kvstore.leader = msg['leader']
                        kvstore.term = msg['term']
                elif msg['term'] == kvstore.term:
                    #heartbeat from expected leader
                    kvstore.last_heartbeat = time.time()
                    index = msg['index']
                    key = msg['key']
                    value = msg['value']
                    
                    if index == kvstore.index:
                        #Good, we're up to date
                        pass
                    elif index == kvstore.index + 1:
                        #Good, this is the next thing we were expecting
                        kvstore.addToLog(key, value)
                        kvstore.put(key, value)
                    elif index > kvstore.index + 1:
                        #woah!, we're behind, lets catch up
                        kvstore.send_request(kvstore.index + 1)
                        
                    else:
                        
                        print msg['src'] + " " + str(msg['index']) + " Something weird is happening " + kvstore.id + " " + str(kvstore.index)
                    
            else:
                if msg['index'] >= kvstore.index:
                    kvstore.vote_in_election(msg['src'])
                    continue
                
        
    clock = time.time()
    if clock - last > .5 and kvstore.leader == kvstore.id:
        #print kvstore.id + ": sending leader heartbeat"
        kvstore.send_RPC()
        last = clock
        
