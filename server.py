import sys
import raft_pb2 as pb2
import raft_pb2_grpc as pb2_grpc
import grpc
import threading, time
from threading import Thread
import random
from concurrent import futures

# Config file has servers addresses that'll be stored in servers.
CONFIG_PATH = "config.conf"  # overwrite this with your config file path
SERVERS = {}

# Server identifying variables.
TERM, VOTED, STATE, VOTES = 0, False, "Follower", 0  
#TERM denotes the current term of the server #VOTED is whether server has votes or not(not sure)
#STATE is current state of the server #VOTES is total number of votes received in leader election
LEADER_ID = None #current leader ID [i think every node knows about it, ideally it should]
IN_ELECTIONS = False #is set true when a server is participating in elections
SERVER_ID = int(sys.argv[1])  #unique identifier of the server within the cluster #passed as command line
VOTED_NODE = -1  #ID of the node votes for
SERVER_ACTIVE = True 

# Time limit in seconds for timouts, and timer to set the time limit for certain functionalities.
TIME_LIMIT = (random.randrange(150, 301) / 1000)
LATEST_TIMER = -1

# Threads used to request votes from servers as a candidate, and to append entries as a leader.
CANDIDATE_THREADS = []
LEADER_THREADS = []

# Commit Index is index of last log entry on server
# Last applied is index of last applied log entry on server
commitIndex, lastApplied, lastLogTerm = 0, 0, 0
# For applied commits.
ENTRIES = {}
# For non-applied commits.
LOGS = []

# NextIndex: list of indices of next log entry to send to server
# MatchIndex: list of indices of latest log entry known to be on every server
nextIndex, matchIndex = [], []
matchTerm = []
n_logs_replicated = 1
# TODO: handle when server is suspended and can't respond to requests
# Handler for RPC functions.

class RaftHandler(pb2_grpc.RaftServiceServicer):

    # This function is called by the Candidate during the elections to collect votes.
    def RequestVote(self, request, context): #request message is sent by candidate to other servers (has term and ID of candidate)
        global TERM, VOTED, STATE, VOTED_NODE, IN_ELECTIONS, LATEST_TIMER #parameters of the candidate server
        global commitIndex, lastLogTerm #later
        candidate_term, candidate_id = request.term, request.candidateId #request msg has term and ID of candidate
        candidateLastLogIndex, candidateLastLogTerm = request.lastLogIndex, request.lastLogTerm #later

        result = False #initially the response is set as false
        IN_ELECTIONS = True #set in election flag as true
        if TERM < candidate_term: #if the term of target server is less than that of candidate, then it would grant vote
            TERM = candidate_term
            result = True
            VOTED = True #VOTED is whether target node has voted for candidate or not
            VOTED_NODE = candidate_id #VOTED_NODE contains ID of candidate/server votes for
            print(f"Voted for node {candidate_id}.")
            run_follower() #This server will now be a follower after voting

        elif TERM == candidate_term: #if term of target server is equal to that of candidate
            if VOTED or candidateLastLogIndex < len(LOGS) or STATE != "Follower": #if VOTED == true #if candidate log not updated #if server not follower
                pass #do not vote
            elif (candidateLastLogIndex == len(LOGS) and (LOGS[candidateLastLogIndex - 1]["TERM"] != candidateLastLogTerm)): #if candidate log not updated
                pass #do not vote
            else: #vote in other cases
                result = True
                VOTED = True
                VOTED_NODE = candidate_id
                print(f"Voted for node {candidate_id}.")
            reset_timer(leader_died, TIME_LIMIT)

        else: #if term of target server is greater than candidate and it is a follower, it will not vote
            if STATE == "Follower":
                reset_timer(leader_died, TIME_LIMIT) #reset the timer

        reply = {"term": TERM, "result": result} # the target server replies with its term and the result ie it votes or not
        return pb2.RequestVoteResponse(**reply) #return the response

    # This function is used by leader to append entries in followers.
    def AppendEntries(self, request, context):  #the request msg contains information regarding leader and log enteries
        #context provides additional context and metadata for the RPC call
        global TERM, STATE, LEADER_ID, VOTED, VOTED_NODE #parameters of follower
        global LATEST_TIMER, commitIndex, ENTRIES, lastApplied

        leader_term, leader_id = request.term, request.leaderId  #term and ID of leader
        prevLogIndex, prevLogTerm = request.prevLogIndex, request.prevLogTerm  #
        entries, leaderCommit = request.entries, request.leaderCommit

        #TODO: you may need to spell out what's in entries
        result = False #the result is set false initially

        if leader_term >= TERM:  #can leader term be less than that of follower?
            # Leader is already in a different term than mine.
            if leader_term > TERM:
                VOTED = False
                VOTED_NODE = -1
                TERM = leader_term
                LEADER_ID = leader_id
                run_follower()

            if prevLogIndex <= len(LOGS):
                result = True
                if len(entries) > 0:
                    LOGS.append({"TERM": leader_term, "ENTRY": entries[0]})

                if leaderCommit > commitIndex:
                    commitIndex = min(leaderCommit, len(LOGS))
                    while commitIndex > lastApplied:
                        key, value = LOGS[lastApplied]["ENTRY"]["key"], LOGS[lastApplied]["ENTRY"]["value"]
                        ENTRIES[key] = value
                        lastApplied += 1

            reset_timer(leader_died, TIME_LIMIT)
        reply = {"term": TERM, "result": result}
        return pb2.AppendEntriesResponse(**reply)

    # This function is called from the client to get leader.
    def GetLeader(self, request, context):
        global IN_ELECTIONS, VOTED, VOTED_NODE, LEADER_ID
        print("Command from client: getleader")
        if IN_ELECTIONS and not VOTED:
            print("None None")
            return pb2.GetLeaderResponse(**{"leaderId": -1, "leaderAddress": "-1"})

        if IN_ELECTIONS:
            print(f"{VOTED_NODE} {SERVERS[VOTED_NODE]}")
            return pb2.GetLeaderResponse(**{"leaderId": VOTED_NODE, "leaderAddress": SERVERS[VOTED_NODE]})

        return pb2.GetLeaderResponse(**{"leaderId": LEADER_ID, "leaderAddress": SERVERS[LEADER_ID]})

    # This function is called from client to suspend server for PERIOD seconds.
    def Suspend(self, request, context):
        global SERVER_ACTIVE
        SUSPEND_PERIOD = int(request.period)
        print(f"Command from client: suspend {SUSPEND_PERIOD}")
        print(f"Sleeping for {SUSPEND_PERIOD} seconds")
        SERVER_ACTIVE = False
        time.sleep(SUSPEND_PERIOD)
        reset_timer(run_server_role(), SUSPEND_PERIOD)
        return pb2.SuspendResponse(**{})

    # This function is called from client to add key, value/append entry to servers.
    def SetVal(self, request, context):
        key, val = request.key, request.value
        global STATE, SERVERS, LEADER_ID, LOGS, TERM
        if STATE == "Follower":
            try:
                channel = grpc.insecure_channel(SERVERS[LEADER_ID])
                stub = pb2_grpc.RaftServiceStub(channel)
                request = pb2.SetValMessage(**{"key": key, "value": val})
                response = stub.SetVal(request)
                return response
            except grpc.RpcError:
                return pb2.SetValResponse(**{"success": False})
        elif STATE == "Candidate":
            return pb2.SetValResponse(**{"success": False})
        else:
            LOGS.append({"TERM": TERM, "ENTRY": {"commandType": "set", "key": request.key, "value": request.value}})
            return pb2.SetValResponse(**{"success": True})
            
    # This function is called from client to get value associated with key.
    def GetVal(self, request, context):
        key = request.key
        global ENTRIES
        if key in ENTRIES:
            val = ENTRIES[key]
            return pb2.GetValResponse(**{"success": True, "value": val})
        return pb2.GetValResponse(**{"success": False})


# Read config file to make the list of servers IDs and addresses.
def read_config(path):
    global SERVERS
    with open(path) as configFile:
        lines = configFile.readlines()
        for line in lines:
            parts = line.split()
            SERVERS[int(parts[0])] = (f"{str(parts[1])}:{str(parts[2])}")


# Runs the behaviour of changing from a follower:
# declares leader dead and becomes a candidate.
def leader_died():  #called when server detects that current leader has died
    global STATE #denotes the current state in the raft algorithm
    if STATE != "Follower": #this means that server is a leader/candidate has has taken action already so return
        return
    print("The leader is dead")
    STATE = "Candidate" #once leader has died, the server becomes a candidate
    run_candidate()

def run_follower():
    global STATE
    STATE = "Follower"
    print(f"I'm a follower. Term: {TERM}")
    reset_timer(leader_died, TIME_LIMIT)

def get_vote(server):  #sends request to another server in leader election #here server is target server
    global TERM, STATE, TIME_LIMIT, VOTES  #these belong to which server, one sending or one receiving?

    try:
        #print("I'm trying to get votes from server x")
        channel = grpc.insecure_channel(server) #security is not a concern in insecure channels
        stub = pb2_grpc.RaftServiceStub(channel) #stub methods can we used to make rpc calls to server #also uses the protocol buffer files
        request = pb2.RequestVoteMessage(**{"term": TERM, "candidateId": SERVER_ID})  #it has the candidate term and its ID
        response = stub.RequestVote(request) #captures response received #what is format of response??
        # print("hey i am printing response")
        # print(response)

        if response.term > TERM:   #if term of target server is greater than term of candidate, set term of candidate as term of target server
            TERM = response.term
            STATE = "Follower" #also, make the candidate a follower
            TIME_LIMIT = (random.randrange(150, 301) / 1000)
            reset_timer(leader_died, TIME_LIMIT)

        if response.result: #if the target server gives you his vote
            VOTES += 1 #increment votes

        return response.result
    except grpc.RpcError:
        pass

def process_votes():
    global STATE, LEADER_ID, CANDIDATE_THREADS, TIME_LIMIT, nextIndex, matchIndex

    for thread in CANDIDATE_THREADS:
        thread.join(0)  ##each thread is joined with a timeout of 0 secs, checking if each thread has completed without waiting

    print("Votes received") ## print that youu have successfully received votes

    if VOTES > len(SERVERS) / 2:
        print(f"I am a leader. Term: {TERM}")
        STATE = "Leader"
        LEADER_ID = SERVER_ID
        # reset_timer(leader_died, TIME_LIMIT)
        nextIndex = [len(LOGS) for i in range(len(SERVERS))]  ##later
        matchIndex = [0 for i in range(len(SERVERS))]  ##later
        run_leader()
    else:
        STATE = "Follower"
        TIME_LIMIT = (random.randrange(150, 301) / 1000)
        run_follower()

# Runs the behaviour of a candidate that reaches out to alive servers and asks them to vote to itself.
# If it gets the majority of votes, it becomes a leader, else, it's downgraded to a follower and runs follower behaviour.
def run_candidate():
    global TERM, STATE, LEADER_ID, LATEST_TIMER, TIME_LIMIT, IN_ELECTIONS, VOTED_NODE
    global SERVER_ID, VOTES, CANDIDATE_THREADS, VOTED

    TERM += 1  #increments term by 1 indicating server is starting a new election in next term
    IN_ELECTIONS = True #indicated server is participating in election
    VOTED_NODE = SERVER_ID #voted node is the node for which the server votes, in this case it votes for itself
    CANDIDATE_THREADS = [] #empty list to store threads for requesting for votes, how???
    VOTES = 1 #current vote count
    VOTED = True #indicates server has votes for itself # not sure 
    print(f"I'm a candidate. Term: {TERM}.\nVoted for node {SERVER_ID}")

    # Requesting votes.
    for key, value in SERVERS.items():
        if SERVER_ID is key: #skip if the ID matches your own ID
            continue
        CANDIDATE_THREADS.append(Thread(target=get_vote, kwargs={'server':value})) 

    # Check if you won the election and can become a leader.
    for thread in CANDIDATE_THREADS:  #how is this working????
        thread.start()
    reset_timer(process_votes, TIME_LIMIT)


def replicate_log(key, server):
    global LOGS, STATE, matchIndex, matchTerm, nextIndex, n_logs_replicated
    leaderCommit = commitIndex
    prevLogIndex = matchIndex[key]
    log=[]
    prevLogTerm = TERM
    if nextIndex[key] < len(LOGS):
        # {"commandType": "set", "key": request.key, "value": request.value}
        log = [{"commandType": "set", "key": LOGS[nextIndex[key]-1]["ENTRY"]["key"], "value": LOGS[nextIndex[key]-1]["ENTRY"]["value"]}]
        prevLogTerm = LOGS[prevLogIndex]["TERM"]
    try:
        channel = grpc.insecure_channel(server)
        stub = pb2_grpc.RaftServiceStub(channel)
        request = pb2.AppendEntriesMessage(**{"term": TERM, "leaderId": SERVER_ID,
                                              "prevLogIndex": prevLogIndex, "prevLogTerm": prevLogTerm,
                                              "entries": log,"leaderCommit": leaderCommit})
        response = stub.AppendEntries(request)
        if(response.term > TERM):
            STATE = "Follower"
            reset_timer(leader_died, TIME_LIMIT)
            run_follower()
        if response.result:
            if log!= []:
                matchIndex[key] = nextIndex[key]
                nextIndex[key] += 1
                n_logs_replicated += 1
        else:
            nextIndex[key] -= 1 
            matchIndex[key] = min(matchIndex[key], nextIndex[key]-1)
    except grpc.RpcError:
        pass


# Runs the behaviour of a leader that sends a heartbeat message after 50 milliseconds.
def run_leader():
    global SERVER_ID, STATE, LEADER_THREADS, n_logs_replicated, nextIndex, lastApplied, commitIndex
    # Send messages after 50 milliseconds.
    LEADER_THREADS = []
    n_logs_replicated = 1
    for key in SERVERS:
        if SERVER_ID is key:
            continue
        LEADER_THREADS.append(Thread(target=replicate_log, kwargs={'key': key, 'server':SERVERS[key]}))
    for thread in LEADER_THREADS:
        thread.start()
    if len(LOGS) > len(ENTRIES):
        key, value = LOGS[lastApplied]["ENTRY"]["key"], LOGS[lastApplied]["ENTRY"]["value"]
        ENTRIES[key] = value
        lastApplied += 1
        commitIndex += 1
    reset_timer(run_server_role, 0.05)

def reset_timer(func, time_limit):
    global LATEST_TIMER
    LATEST_TIMER.cancel()
    LATEST_TIMER = threading.Timer(time_limit, func)  #this line indicates that a new timer object is created with specified time limit and once the timer expires, func is called
    LATEST_TIMER.start()

def run_server_role():
    global SERVER_ACTIVE
    SERVER_ACTIVE = True
    if(STATE == "Leader"):
        run_leader()
    elif STATE == "Candidate":
        run_candidate()
    else:
        run_follower()



# Initializes the server. Every server starts as a follower.
def run_server():
    global TERM, STATE, SERVERS, TIME_LIMIT, LATEST_TIMER
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10)) #initializes a new grpc server instance
    pb2_grpc.add_RaftServiceServicer_to_server(RaftHandler(), server)
    server.add_insecure_port(SERVERS[SERVER_ID])  #This method is used to add a port for the gRPC server to listen on

    # print follower start
    print(f"I'm a follower. Term: {TERM}")
    LATEST_TIMER = threading.Timer(TIME_LIMIT, leader_died)
    LATEST_TIMER.start()
    try:
        server.start()
        while (True):
            server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"Server {SERVER_ID} is shutting down")


if __name__ == '__main__':
    read_config(CONFIG_PATH)
    print(f"{SERVERS[SERVER_ID]}")
    run_server()