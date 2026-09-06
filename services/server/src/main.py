import os
import signal
import sys
import threading

import logger
import server

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
AGENCY_QUORUM_MIN = int(os.environ.get("AGENCY_QUORUM_MIN"))

def main():
    logger.init()
    sigterm_arrived = threading.Event()
    def sigterm_handler(signum, frame):
        logger.info("sigterm-handler", logger.LogResult.success, "SIGTERM received")
        sigterm_arrived.set()
    signal.signal(signal.SIGTERM, sigterm_handler)
    s = server.Server(SERVER_HOST, SERVER_PORT, AGENCY_QUORUM_MIN)
    try:
        s.run(sigterm_arrived)
    except Exception as e:
        logger.error("server-run", logger.LogResult.fail, "err", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
