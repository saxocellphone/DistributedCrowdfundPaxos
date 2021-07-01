import logging, sys
import json
import threading
import socket
from consts import PaxosEvent
from paxos_site import AbstractSiteDecorator

class Acceptor(AbstractSiteDecorator):
    def __init__(self, decorated_site):
        super().__init__(decorated_site)
        self.port = self.decorated_site.site_dict[self.decorated_site.site_id]["udp_start_port"] + 1
        logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    def message_reciever(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_socket.bind(("", self.port))
        while True:
            try:
                message, address = server_socket.recvfrom(32768)
                decoded_message = json.loads(message.decode("utf-8"))
                event = decoded_message["event"]
                if event == PaxosEvent.PREPARE:
                    res = self.promise(decoded_message["propose_num"], decoded_message["log_slot"])
                elif event == PaxosEvent.ACCEPT:
                    res = self.accept(decoded_message["propose_num"], decoded_message["propose_val"], decoded_message["log_slot"])
                else:
                    raise Exception("Unknown event for acceptor!")
                server_socket.sendto(json.dumps(res).encode("utf-8"), address)
            except Exception as e:
                logging.info(f"Acceptor: Error while receiving: {e}")

    def promise(self, propose_num, log_slot):
        logging.info(f"received prepare({propose_num}) for slot {log_slot}")
        if propose_num > self.decorated_site.max_prepare[log_slot]:
            self.decorated_site.max_prepare[int(log_slot)] = propose_num
            ret_obj =  {
                "origin"      : self.decorated_site.site_id,
                "event"       : PaxosEvent.PROMISE,
                "accepted_num": self.decorated_site.accepted_num[log_slot],
                "accepted_val": self.decorated_site.accepted_val[log_slot]
                }
            logging.info(f"send promise({ret_obj['accepted_num']}, {ret_obj['accepted_val']}) to slot {log_slot}")
        else:
            ret_obj = {
                "origin"     : self.decorated_site.site_id,
                "event"      : PaxosEvent.NACK,
                "max_num"    : self.decorated_site.max_prepare[log_slot]
                }
            logging.info(f"send nack({ret_obj['max_num']}) at slot {log_slot}")
        return ret_obj

    def accept(self, prop_num, prop_val, log_slot):
        logging.info(f"received prepare({prop_num}) for slot {log_slot}")
        if prop_num >= self.decorated_site.max_prepare[log_slot]:
            self.decorated_site.max_prepare[log_slot] = prop_num
            self.decorated_site.accepted_val[int(log_slot)] = prop_val
            self.decorated_site.accepted_num[int(log_slot)] = prop_num
            ret_obj =  {
                "origin"      : self.decorated_site.site_id,
                "event"       : PaxosEvent.ACCEPTED,
                "accepted_num": self.decorated_site.accepted_num[log_slot],
                "accepted_val": self.decorated_site.accepted_val[log_slot],
                "log_slot"    : log_slot
                }
            logging.info(f"send accept({ret_obj['accepted_num']}, {ret_obj['accepted_val']}) to slot {log_slot}")
        else:
            ret_obj = {
                "origin"     : self.decorated_site.site_id,
                "event"      : PaxosEvent.NACK,
                "max_num"    : self.decorated_site.max_prepare
                }
            logging.info(f"Acceptor: Can't accept proposal# {prop_num} at slot {log_slot}")
        return ret_obj

    def listen(self):
        server_thread = threading.Thread(target=self.message_reciever)
        server_thread.daemon = True
        server_thread.start()
        logging.info(f"Acceptor: Now listening on port {self.port}.")

