import signal
import logging
import sys
import paxos_site
from proposer import Proposer
from acceptor import Acceptor
from learner  import Learner

def main():
    signal.signal(signal.SIGINT, lambda _: sys.exit(0))
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

    mysite = paxos_site.Site()

    myacceptor = Acceptor(mysite)
    myacceptor.listen()

    mylearner = Learner(mysite)
    mylearner.listen()

    myproposer = Proposer(mysite)
    
    while True:
        
        text = input().strip(" \n")
        split_text = text.split(" ")
        command = split_text[0]
        if command == "quit":
            break
        elif command == "log":
            mysite.log()
            
        elif command == "projects":
            mysite.projects()
            
        elif command == "list" and len(split_text) == 2:
            mysite.list_out(split_text[1])
            
        elif command == "create" and len(split_text) == 3:
            myproposer.create_project(split_text[1], int(split_text[2]))
            
        elif command == "pledge" and len(split_text) == 3:
            myproposer.create_pledge(split_text[2], split_text[1])
            
        elif command == "withdraw" and len(split_text) == 2:
            myproposer.withdraw_pledge(split_text[1])
            
        elif command == "cancel" and len(split_text) == 2:
            myproposer.cancel_project(split_text[1])
            
        elif command == "debug":
            mysite.debug()

        elif command == "rm" and len(split_text) == 2:
            mysite.remove_log_entry(int(split_text[1]))
            
        else:
            print("Check your input!")

    sys.exit()

if __name__ == "__main__":
    main()
