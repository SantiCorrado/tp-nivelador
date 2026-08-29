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

class Connection(threading.Thread):
    def __init__(self, client_socket):
        super().__init__()
        self.client_socket = client_socket
        self.agency_id = None
        self.error = None

    def run(self):
        try:
            self.handle_client(self.client_socket)
        except Exception as e:
            self.error = e

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

    def handle_client(self, client_socket) :
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
                    self.agency_id = agency
                elif message_type == _TYPE_BET:
                    client_message = self.handle_bets(client_socket, length)
                    bets = self.parse_bets(client_message, agency)
                    lottery.store_bets(bets)
                    message_amount += 1
                elif message_type == _TYPE_END:
                    break
                
            final_message = (f"Agency: {agency}, Messages received: {message_amount}\n")
            logger.info(action, logger.LogResult.success, final_message)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

class Server:
    def __init__(self, server_host: str, server_port: int, batch_size: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.batch_size = batch_size
        self.agency_quorum_min = agency_quorum_min
        self.connections = []
        self.succesful_connections = {}

    def bets_csv(self, bets: list[Bet]) -> str:
        action = "bets-csv"
        logger.info(action, logger.LogResult.in_progress)
        csv_lines = []
        for bet in bets:
            csv_lines.append(
                f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
            )
        return "\n".join(csv_lines)

    def send_winners(self, succesful_connections, lottery: Lottery):
        action = "send-winners"
        logger.info(action, logger.LogResult.in_progress)
        winners = {}
        i = 0
        for bet in lottery.load_bets():
            if lottery.has_won(bet) and bet.agency_id in succesful_connections:
                if bet.agency_id not in winners:
                    winners[bet.agency_id] = [bet]
                else:
                    winners[bet.agency_id].append(bet)
                i += 1
                if len(winners[bet.agency_id]) >= self.batch_size:
                    winners_csv = self.bets_csv(winners[bet.agency_id])
                    message_length = len(winners_csv.encode("utf-8"))
                    header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
                    safe_socket.send_all(succesful_connections[bet.agency_id], header)
                    safe_socket.send_all(succesful_connections[bet.agency_id], winners_csv.encode("utf-8"))
                    winners[bet.agency_id] = []
        for w in winners:
            if winners[w]:
                winners_csv = self.bets_csv(winners[w])
                message_length = len(winners_csv.encode("utf-8"))
                header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
                safe_socket.send_all(succesful_connections[w], header)
                safe_socket.send_all(succesful_connections[w], winners_csv.encode("utf-8"))
                
        for agency_id in succesful_connections:
            header = bytes([_TYPE_END]) + (0).to_bytes(4, byteorder="big")
            safe_socket.send_all(succesful_connections[agency_id], header)
            logger.info(action, logger.LogResult.success, "winners-sent", agency_id)

        

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            connection_number = 0
            needed_conections = self.agency_quorum_min
            while connection_number < needed_conections:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    connection_number += 1
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)
                new_connection = Connection(client_socket)
                self.connections.append(new_connection)
                new_connection.start()
                
            for c in self.connections:
                c.join()
                if c.error is None:
                    self.succesful_connections[c.agency_id] = c.client_socket
            if len(self.succesful_connections) >= self.agency_quorum_min:
                self.send_winners(self.succesful_connections, Lottery("bets.csv"))
                # aca se manejarian los siguientes pasos. cierre de conexiones, etc
