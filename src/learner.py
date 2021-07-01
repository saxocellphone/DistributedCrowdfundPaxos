import logging, sys
import json
import threading
import socket
import traceback
from consts import PaxosEvent, UserAction
from paxos_site import AbstractSiteDecorator

class Learner(AbstractSiteDecorator):
    def __init__(self, decorated_site):
        super().__init__(decorated_site)
        self.port = self.decorated_site.site_dict[self.decorated_site.site_id]["udp_start_port"] + 2
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def message_reciever(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(("", self.port))

        while True:
            try:
                message, address = server_socket.recvfrom(32768)
                decoded_message = json.loads(message.decode("utf-8"))
                event = decoded_message["event"]
                if event == PaxosEvent.COMMIT:
                    res = self.learn(decoded_message["commit_val"], decoded_message["log_slot"])
                elif event == PaxosEvent.SEEK:
                    res = self.seek()
                else:
                    raise Exception("Unknown event for learner!")
                server_socket.sendto(json.dumps(res).encode("utf-8"), address)
            except Exception as e:
                logging.info(f"Learner: Error while receiving: {e}")
                logging.info(traceback.format_exc())

    def listen(self):
        server_thread = threading.Thread(target=self.message_reciever)
        server_thread.daemon = True
        server_thread.start()
        logging.info(f"Learner: Now listening on port {self.port}.")

    def seek(self):
        logging.info("received seek (learner)")
        return {
                "event"   : PaxosEvent.ACK,
                "cur_slot": self.decorated_site.cur_log_slot()
                }

    def learn(self, val, log_slot):
        logging.info(f"received commit{val} for slot {log_slot}")
        if log_slot >= len(self.decorated_site.p_log) or self.decorated_site.p_log[log_slot] == None:
            # Add to log only if it's None
            self.decorated_site.safe_add_log(val, log_slot)
            if val["action"] == UserAction.CREATE_PROJECT:
                if val["project_name"] not in self.decorated_site.cancelled_projects:
                    self.decorated_site.crowdfund[val["project_name"]] = val["funding_goal"]
                    self.decorated_site.pledges[val["project_name"]] = self.decorated_site.ghost_pledged[val["project_name"]]
                    del self.decorated_site.ghost_pledged[val["project_name"]]

            elif val["action"] == UserAction.CREATE_PLEDGE:
                if val["pledge_id"] in self.decorated_site.cancelled_pledges:
                    pass
                elif val["project_name"] in self.decorated_site.crowdfund:
                    # If we have officialy created the project, and it's not cancelled
                    l = [val["pledge_id"], val["site_name"]]
                    self.decorated_site.pledges[val["project_name"]].append(l)
                else:
                    # If we receive pledge without officially created project
                    self.decorated_site.ghost_pledged[val["project_name"]].append(val)

            elif val["action"] == UserAction.CANCEL_PROJECT:
                self.decorated_site.cancelled_projects.append(val["project_name"])
                if val["project_name"] in self.decorated_site.crowdfund:
                    self.decorated_site.crowdfund.pop(val.get("project_name"))
                    self.decorated_site.pledges.pop(val.get("project_name"))      

            elif val["action"] == UserAction.WITHDRAW_PLEDGE:
                self.decorated_site.cancelled_pledges.append(val["pledge_id"])
                if val["project_name"] in self.decorated_site.crowdfund:
                    for v in self.decorated_site.pledges[val["project_name"]]:
                        if v[0] == val["pledge_id"]:
                            self.decorated_site.pledges[val["project_name"]].remove(v)
                            break

            self.decorated_site.write_log()
