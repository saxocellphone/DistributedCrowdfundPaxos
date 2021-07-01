from enum import Enum

class UserAction(int, Enum):
    CREATE_PROJECT  = 1
    CANCEL_PROJECT  = 2
    CREATE_PLEDGE   = 3
    WITHDRAW_PLEDGE = 4

class PaxosEvent(int, Enum):
    PREPARE = 1 
    PROMISE = 2
    ACCEPT  = 3
    ACCEPTED= 4
    ACK     = 5 
    NACK    = 6
    COMMIT  = 7
    SEEK    = 8 # For hole filling. Not sending any value, just seeking missed values.

class Event(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))
