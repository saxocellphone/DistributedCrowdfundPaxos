from abc import ABC, abstractmethod
import logging, sys
import sys
import json
import os.path
from collections import defaultdict
from consts import UserAction

class AbstractSite(ABC):
    def __init__():
        pass
    
    @abstractmethod
    def list_out(self):
        pass

    @abstractmethod
    def projects(self):
        pass

    @abstractmethod
    def log(self):
        pass


class Site(AbstractSite):
    def __init__(self):
        # Site initialization
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
        site_name = sys.argv[1] if len(sys.argv) != 1 else "alpha"
        self.site_name = site_name
        with open("knownhosts.json") as f:
            site_dict = json.load(f)["hosts"]
        count = 0
        new_site_dict = {}
        for key in site_dict:
            if key == site_name:
                self.site_id = count
                self.ip_address = site_dict[key]["ip_address"]
            new_site_dict[count] = site_dict[key]
            new_site_dict[count]["site_name"] = key
            count += 1
        self.site_dict = new_site_dict
        self.num_sites = len(site_dict)
        self.majority = self.num_sites // 2 + 1

        # Crowdfund values
        self.crowdfund = {} # key: project_name. value: funding_goal
        self.pledges   = defaultdict(list) # key: project_name. value: list of pledges to project_name
        self.cancelled_projects = []
        self.cancelled_pledges = []
        self.ghost_pledged = defaultdict(list)

        log_file = f"logs/paxos_log{self.site_id}.json"
        if os.path.isfile(log_file) and "debug" not in sys.argv:
            with open(log_file, 'r') as openfile:
                json_obj = json.load(openfile)
                self.p_log = json_obj["p_log"]
                self.pledge_counter = json_obj["pledge_counter"]
                self.proposal_counter = json_obj["proposal_counter"]
                self.max_prop_num = defaultdict(lambda: float(f"1.{self.site_id}"))
                # It's fucking ugly and idc care more
                # Needs the key to be integers!
                for k, v in json_obj["max_prop_num"].items():
                    self.max_prop_num[int(k)] = v

                self.max_prepare = defaultdict(int, json_obj["max_prepare"])
                for k, v in json_obj["max_prepare"].items():
                    self.max_prepare[int(k)] = v

                self.accepted_num = defaultdict(lambda: None, json_obj["accepted_num"])
                for k, v in json_obj["accepted_num"].items():
                    self.accepted_num[int(k)] = v

                self.accepted_val = defaultdict(lambda: None, json_obj["accepted_val"])
                for k, v in json_obj["accepted_val"].items():
                    self.accepted_val[int(k)] = v

                self.holes = set(json_obj["holes"])
                logging.info(f"Site {self.site_name} loaded")
            # TODO: Refactor this
            for entry in self.p_log[:-1]:
                # p_log guarentees validity
                action = entry["action"]
                if action == UserAction.CREATE_PROJECT:
                    self.crowdfund[entry["project_name"]] = entry["funding_goal"]
                elif action == UserAction.CREATE_PLEDGE:
                    self.pledges[entry["project_name"]].append([entry["pledge_id"], entry["site_name"]])
                elif action == UserAction.WITHDRAW_PLEDGE:
                    old = self.pledges[entry["project_name"]]
                    new = []
                    for k in old:
                        if k[0] != entry["pledge_id"]:
                            new.append(k)
                    self.pledges[entry["project_name"]] = new
                elif action == UserAction.CANCEL_PROJECT:
                    del self.pledges[entry["project_name"]]
                    del self.crowdfund[entry["project_name"]]

        else:
            self.pledge_counter = 1
            self.proposal_counter = 0

            # Proposer Values
            # Note: Local max_prop_num is int, but turns into float when sending. 
            #       In the format of {max_prop_num}.{site_id}. For example, 
            #       max_prop_num = 5 and site_it = 2 turns into 5.2
            self.max_prop_num = defaultdict(lambda: float(f"1.{self.site_id}"))
            
            # Acceptor Values
            # Note: All three dicts should have the same key at all time!
            self.max_prepare = defaultdict(int)
            self.accepted_num = defaultdict(lambda: None)
            self.accepted_val = defaultdict(lambda: None)

            # Learner Values
            self.p_log = [None] # There's always an empty 'hole' at the end of the list for future proposals
            self.holes = set([0])  # Set of integers. Keeps track of all the holes, since it's a set it's in assending order.

            logging.info(f"Site {self.site_name} started")

    def debug(self):
        print(self.p_log, self.cur_log_slot())

    def remove_log_entry(self, slot):
        # DEBUG FUNCTION DO NOT USE
        self.holes.add(slot)
        self.p_log[slot] = None

    def write_log(self):
        log_file = f"logs/paxos_log{self.site_id}.json" 
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "w") as openfile:
            #TODO: Don't need to open everytime you learn
            dictionary = {
                "p_log"           : self.p_log,
                "max_prop_num"    : self.max_prop_num,
                "max_prepare"     : self.max_prepare,
                "accepted_num"    : self.accepted_num,
                "accepted_val"    : self.accepted_val,
                "holes"           : list(self.holes),
                "pledge_counter"  : self.pledge_counter,
                "proposal_counter": self.proposal_counter
                }
            json.dump(dictionary, openfile)
            openfile.close()

    def list_out(self, project):
        proj_pledges = self.pledges.get(project)
        if proj_pledges != None:
            for pledge in proj_pledges:
                print(pledge[0], pledge[1])

    def projects(self):
        for project in sorted(self.crowdfund.keys()):
            status = "unfunded"
            if self.crowdfund.get(project) == len(self.pledges.get(project))*100:
                status = "funded"
            print(project, self.crowdfund.get(project), status)

    def log(self):
        for entry in self.p_log[:-1]:
            sentence = ""
            if not entry:
                sentence = "missing entry"
            elif entry["action"] == UserAction.CREATE_PROJECT:
                sentence = "create_project " + entry["project_name"] + " " + entry["proposal_id"].rsplit('_')[0] + " " + str(entry["funding_goal"])
            elif entry["action"] == UserAction.CREATE_PLEDGE:
                sentence = "make_pledge " + entry["pledge_id"] + " " + entry["project_name"] + " " + entry["proposal_id"].rsplit('_')[0]
            elif entry["action"] == UserAction.CANCEL_PROJECT:
                sentence = "cancel_project " + entry["project_name"]
            elif entry["action"] == UserAction.WITHDRAW_PLEDGE:
                sentence = "withdraw_pledge " + entry["pledge_id"]
            print(sentence)
                

    def safe_add_log(self, val, slot):
        if len(self.p_log) - 1 <= slot:
            for h in range(len(self.p_log), slot + 2):
                if h != slot:
                    self.holes.add(h)
            self.p_log.extend([None for _ in range(slot - len(self.p_log) + 2)])
        if slot in self.holes:
            self.holes.remove(slot)
        self.p_log[slot] = val

    def cur_log_slot(self):
        # Return the slot for the next proposal
        return min(self.holes)

class AbstractSiteDecorator():
    def __init__(self, decorated_site):
        self.decorated_site = decorated_site

    def list_out(self):
        self.decorated_site.list_out()

    def projects(self):
        self.decorated_site.projects()

    def log(self):
        self.decorated_site.log()

