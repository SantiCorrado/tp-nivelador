import socket
import logger
import safe_socket
from src_frozen.lottery.lottery import Lottery
from src_frozen.lottery.bet import Bet
_ECHO_SERVER_MESSAGE_SIZE = 1024

_HEADER_LENGTH = 5
_TYPE_GREETINGS = 1
_TYPE_BET = 2
_TYPE_END = 3

class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def handle_header(self, header: bytes):
        action = "handle-header"
        logger.info(action, logger.LogResult.in_progress)
        message_type = header[0]
        length = int.from_bytes(header[1:5], byteorder="big")
        return message_type, length

    def handle_bets(self, client_socket, length):
        action = "handle-bet"
        logger.info(action, logger.LogResult.in_progress)
        client_message = safe_socket.recv_all(client_socket, length)
        return client_message.decode("utf-8")

    def handle_greetings(self, client_socket, length):
        action = "handle-greetings"
        logger.info(action, logger.LogResult.in_progress)
        client_message = safe_socket.recv_all(client_socket, length)
        return int.from_bytes(client_message, byteorder="big")

    def parse_bets(message: str, agency_id: int) -> list[Bet]:
        action = "parse-bets"
        logger.info(action, logger.LogResult.in_progress)
        bets = []
        for line in message.splitlines():
            col = line.split(",")
            bet = Bet(
                agency_id,
                col[0],
                col[1],
                int(col[2]),
                col[3],
                int(col[4])
            )
            bets.append(bet)

        return bets


    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency = -1
        length = 0
        lottery = Lottery("bets.csv")
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                header = safe_socket.recv_all( client_socket, _HEADER_LENGTH)
                if not header:
                    break

                message_type, length = self.handle_header(header)

                if message_type == _TYPE_GREETINGS:
                    agency = self.handle_greetings(client_socket, length)
                elif message_type == _TYPE_BET:
                    client_message = self.handle_bets(client_socket, length)
                    bets = self.parse_bets(client_message, agency)
                    lottery.store_bet(bets)
                    message_amount += 1
                elif message_type == _TYPE_END:
                    break
                
            final_message = (f"Agency: {agency}, Messages received: {message_amount}\n").encode("utf-8")
            safe_socket.send_all(client_socket, final_message)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            client_socket.close()
        

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
