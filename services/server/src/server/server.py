import os
import socket
import threading
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

_HEADER_LENGTH = 5
_TYPE_GREETINGS = 1
_TYPE_BET = 2
_TYPE_END = 3
_TYPE_WINNERS = 4

class Server:
    def __init__(self, server_host: str, server_port: int, batch_size: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.batch_size = batch_size
        self.agency_quorum_min = agency_quorum_min

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

    def parse_bets(self, message: str, agency_id: int) -> list[Bet]:
        action = "parse-bets"
        logger.info(action, logger.LogResult.in_progress)
        bets = []
        for line in message.splitlines():
            col = line.split(",")
            if len(col) != 5:
                logger.error(action, logger.LogResult.fail, "bet mal formateada", line)
                continue
            bet = Bet(agency_id,col[0],col[1],int(col[2]),col[3],int(col[4]))
            bets.append(bet)

        return bets

    def bets_csv(self, bets: list[Bet]) -> str:
        action = "bets-csv"
        logger.info(action, logger.LogResult.in_progress)
        csv_lines = []
        for bet in bets:
            csv_lines.append(
                f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
            )
        return "\n".join(csv_lines)

    def send_winners(self, client_socket, lottery: Lottery, agency_id: int):
        action = "send-winners"
        logger.info(action, logger.LogResult.in_progress)
        winners = []
        i = 0
        for bet in lottery.load_bets():
            if lottery.has_won(bet) and bet.agency_id == agency_id:
                winners.append(bet)
                i += 1
            if len(winners) >= self.batch_size:
                winners_csv = self.bets_csv(winners)
                message_length = len(winners_csv.encode("utf-8"))
                header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
                safe_socket.send_all(client_socket, header)
                safe_socket.send_all(client_socket, winners_csv.encode("utf-8"))
                winners = []
        if len(winners) > 0:
            winners_csv = self.bets_csv(winners)
            message_length = len(winners_csv.encode("utf-8"))
            header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
            safe_socket.send_all(client_socket, header)
            safe_socket.send_all(client_socket, winners_csv.encode("utf-8"))
        header = bytes([_TYPE_END]) + (0).to_bytes(4, byteorder="big")
        safe_socket.send_all(client_socket, header)
        logger.info(action, logger.LogResult.success, "winners-sent", i)
        


    def _handle_client(self, client_socket) :
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
                    lottery = Lottery("bets" + str(agency) + ".csv")
                elif message_type == _TYPE_BET:
                    client_message = self.handle_bets(client_socket, length)
                    bets = self.parse_bets(client_message, agency)
                    lottery.store_bets(bets)
                    message_amount += 1
                elif message_type == _TYPE_END:
                    break
                
            final_message = (f"Agency: {agency}, Messages received: {message_amount}\n")
            logger.info(action, logger.LogResult.success, final_message)
            self.send_winners(client_socket, lottery, agency)
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
            input_agecies = set()
            quorum = threading.Condition()
            connections = 0
            while connections < self.agency_quorum_min:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
                connections += 1
            