import logging, sys
import time
import json
import threading, queue
import socket
from consts import PaxosEvent, UserAction
from paxos_site import AbstractSiteDecorator
from collections import defaultdict

class Proposer(AbstractSiteDecorator):
    def __init__(self, decorated_site):
        super().__init__(decorated_site)
        self.port = self.decorated_site.site_dict[self.decorated_site.site_id]["udp_start_port"]
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
        
        # Recovery 
        obj = {
            "event"     : PaxosEvent.SEEK
            }
        res, _ = self.send_to_majority(obj, port_offset=2)
        max_slot = self.decorated_site.cur_log_slot()
        for r in res:
            max_slot = max(r["cur_slot"], max_slot)
        for i in range(self.decorated_site.cur_log_slot() + 1, max_slot + 1):
            self.decorated_site.holes.add(i)

        try:
            self.fill_hole()
        except Exception as e:
            logging.info(f"exceiption while filling holes, {e}")


    def send_to_majority(self, obj, port_offset=1):
        counter = Counter(self.decorated_site.majority)
        resQ = queue.Queue()
        threads = []
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        def send_helper(destination, obj, counter):
            addr = (self.decorated_site.site_dict[destination]["ip_address"], \
                    self.decorated_site.site_dict[destination]["udp_start_port"] + port_offset) 
            client_socket.sendto(json.dumps(obj).encode("utf-8"), addr)
            try:
                if counter.reached_target():
                    # Already reached majority, no further need
                    return
                client_socket.settimeout(0.2)
                message, _ = client_socket.recvfrom(32768)
                decoded_message = json.loads(message.decode("utf-8"))
                counter.increse()
                resQ.put(decoded_message)
                resQ.task_done()
            except:
                return

        for k in self.decorated_site.site_dict.keys():
            pass_arg = [k, obj, counter]
            t = threading.Thread(target=send_helper, daemon=True, args=(*pass_arg,))
            t.start()
            threads.append(t)
        
        start = time.time()
        while not counter.reached_target():
            if time.time() - start > 2:
                # Two second timeout
                break
        for t in threads:
            t.join()
        res = []
        max_nack = 0
        while not resQ.empty():
            r = resQ.get()
            if r["event"] != PaxosEvent.NACK:
                res.append(r)
            else:
                max_nack = max(max_nack, int(r["max_num"]))
                
        resQ.join()
        # TODO: Don't change max_nack's suffix here maybe?
        return (res, float(f"{max_nack}.{self.decorated_site.site_id}"))

    def commit(self, val, log_slot):
        # Called when the proposer acts as Distinguished Learner
        commit_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for _, site in self.decorated_site.site_dict.items():
            addr = (site["ip_address"], site["udp_start_port"] + 2) # +2 cuz it's the learners
            obj = {
                "event"     : PaxosEvent.COMMIT,
                "commit_val": val,
                "log_slot"  : log_slot
                }
            commit_socket.sendto(json.dumps(obj).encode("utf-8"), addr)

    def prepare(self, log_slot, try_num=0, max_try=3):
        if try_num >= max_try:
            raise Exception("Max number of tries exceeded")
        logging.info(f"sending prepare({self.decorated_site.max_prop_num[log_slot]}) to slot {log_slot}")
        obj = {
                "event"      : PaxosEvent.PREPARE,
                "propose_num": self.decorated_site.max_prop_num[log_slot],
                "log_slot"   : log_slot
                }
        res, max_nack = self.send_to_majority(obj)

        if len(res) < self.decorated_site.majority:
            new_num = max(self.decorated_site.max_prop_num[log_slot], max_nack) + 1
            logging.info(f"(retry) sending prepare({new_num}) to slot {log_slot}")
            self.decorated_site.max_prop_num[log_slot] = new_num
            return self.prepare(log_slot, try_num + 1)
        return (res, try_num)

    def accept(self, promised_sites, proposed_value, log_slot, try_num=0):
        assert len(promised_sites) >= self.decorated_site.majority
        original_prop_num = self.decorated_site.max_prop_num[log_slot]

        if all([r["accepted_num"] == None for r in promised_sites]):
            accepted_prop_val = proposed_value
        else:    
            largest_num = None
            accepted_prop_val = None
            for r in promised_sites:
                if r["event"] != PaxosEvent.PROMISE:
                    raise Exception("Something went wrong during second phase of proposal")
                if r["accepted_num"] and (not largest_num or r["accepted_num"] > largest_num):
                    accepted_prop_val = r["accepted_val"]

        obj = {
                "event"      : PaxosEvent.ACCEPT,
                "propose_num": original_prop_num,
                "propose_val": accepted_prop_val,
                "log_slot"   : log_slot
                }
        logging.info(f"sending accept({original_prop_num}, {accepted_prop_val}) to slot {log_slot}")
        res, max_nack = self.send_to_majority(obj)
        if len(res) < self.decorated_site.majority:
            new_num = max(self.decorated_site.max_prop_num[log_slot] + 1, max_nack)
            self.decorated_site.max_prop_num[log_slot] = new_num
            logging.info(f"Proposer: Proposal# {self.decorated_site.max_prop_num[log_slot]} too small, retrying with {new_num}")
            res, num_tries = self.prepare(try_num + 1)
            self.accept(res, proposed_value, log_slot, try_num=num_tries)
        else:
            # Proposer acts as distinguished learner
            counter = defaultdict(int)
            max_count = -1
            commit_val = None
            for r in res:
                assert r["event"] == PaxosEvent.ACCEPTED, f"DL event wrong. Expected {PaxosEvent.ACCEPT} got {r['event']}"
                assert r["log_slot"] == log_slot, f"DL Log Slot wrong. Expected {log_slot}, got {r['log_slot']}"
                counter[r["accepted_num"]] += 1
                if counter[r["accepted_num"]] > max_count:
                    commit_val = r["accepted_val"]
                    max_count = counter[r["accepted_num"]]
            if max_count >= self.decorated_site.majority:
                self.commit(commit_val, log_slot)
                return commit_val["proposal_id"] == proposed_value["proposal_id"] # If learned entry is the same as proposed, then we know proposal has gone through
        return False

    def fill_hole(self):
        for hole in list(self.decorated_site.holes)[:-1]:
            try:
                res, _ = self.prepare(hole)
                commit_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                addr = ("127.0.0.1", self.decorated_site.site_dict[self.decorated_site.site_id]["udp_start_port"] + 2) # +2 cuz it's the learners
                commit_val = max(res, key=lambda x:x["accepted_num"] if x["accepted_num"] else -1)["accepted_val"]
                if commit_val:
                    obj = {
                        "event"     : PaxosEvent.COMMIT,
                        "commit_val": commit_val,
                        "log_slot"  : hole
                        }
                    commit_socket.sendto(json.dumps(obj).encode("utf-8"), addr)
            except Exception as e:
                logging.info(f"Proposal failed for slot {hole}")
             

    def propose(self, proposed_value):
        proposed_value["proposal_id"] = f"{self.decorated_site.site_name}_{self.decorated_site.proposal_counter}"
        self.decorated_site.proposal_counter = self.decorated_site.proposal_counter + 1
        try:
            res, num_tries = self.prepare(self.decorated_site.cur_log_slot())
            committed = self.accept(res, proposed_value, self.decorated_site.cur_log_slot(), try_num=num_tries)
            if not committed:
                raise Exception("Proposal failed.")
            return True
        except Exception as e:
            logging.info(e)
            return False

    def create_project(self, project_name, funding_goal):
        self.fill_hole()
        if project_name in self. decorated_site.crowdfund:
            print(f"Unable to create {project_name}, already exists.")
        else:
            self.fill_hole()
            proposed_value = {
                    "action"      : UserAction.CREATE_PROJECT,
                    "project_name": project_name,
                    "funding_goal": funding_goal
                    }
            if self.propose(proposed_value):
                print(f"Created project {project_name}.")
            else:
                print(f"Unable to create {project_name}.")

    def cancel_project(self, project_name):
        print(self.decorated_site.pledges.get(project_name))
        print(self.decorated_site.crowdfund.get(project_name))
        self.fill_hole()
        if self.decorated_site.pledges.get(project_name) == None:
            print(f"Can't find project {project_name}")
        elif len(self.decorated_site.pledges.get(project_name))*100 < self.decorated_site.crowdfund.get(project_name):
            proposed_value = {
                    "action"      : UserAction.CANCEL_PROJECT,
                    "project_name": project_name
                    }   
            pledges = list(self.decorated_site.pledges[proposed_value["project_name"]])
            withdraw_failed = False
            for pledge in pledges:
                withdraw_proposed_value = {
                        "action"      : UserAction.WITHDRAW_PLEDGE,
                        "pledge_id"   : pledge[0],
                        "project_name": project_name
                        }
                if not self.propose(withdraw_proposed_value):
                    withdraw_failed = True
            if withdraw_failed:
                print("Some pledges withdrawn, but unable to cancel " + proposed_value["project_name"] + ".")
            elif self.propose(proposed_value):
                print("Project " + proposed_value["project_name"] + " cancelled.")
            else:
                print(f"Unable to cancel {project_name}.")
        else:
            print(f"Unable to cancel {project_name}.")
            

    def create_pledge(self, project_name, user_id):
        self.fill_hole()
        pledge_id = f"{user_id}{self.decorated_site.pledge_counter}"
        if project_name not in self.decorated_site.crowdfund:
            print(f"Project {project_name} not found!")
        else:
            if len(self.decorated_site.pledges.get(project_name))*100 < self.decorated_site.crowdfund.get(project_name):
                proposed_value = {
                        "action"      : UserAction.CREATE_PLEDGE,
                        "project_name": project_name,
                        "pledge_id"   : pledge_id,
                        "site_name"   : self.decorated_site.site_name,
                        }
                if self.propose(proposed_value):
                    print(f"Created pledge {pledge_id} to {project_name}.")
                    self.decorated_site.pledge_counter += 1
                else:
                    print(f"Cannot create pledge to {project_name}.")
            else:
                print(f"Cannot create pledge to {project_name}.")

    def withdraw_pledge(self, pledge_id):
        self.fill_hole()
        for key,value in self.decorated_site.pledges.items():
            for v in value:
                if pledge_id == v[0]:
                    proposed_value = {
                            "action"      : UserAction.WITHDRAW_PLEDGE,
                            "pledge_id"   : pledge_id,
                            "project_name": key
                            }
                    if len(value)*100 < self.decorated_site.crowdfund[key]:
                        if self.propose(proposed_value):
                            print(f"Withdrew pledge {pledge_id}.")
                            return
                        else:
                            print(f"Cannot withdraw {pledge_id}.")
                            return
        print(f"Pledge {pledge_id} not found")

class Counter():
    def __init__(self, target):
        self.count = 0
        self.lock = threading.Lock()
        self.target = target

    def increse(self):
        with self.lock:
            self.count += 1

    def reached_target(self):
        return self.count >= self.target
