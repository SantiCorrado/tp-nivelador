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

_CONN_BATCH = 10

class Connection(threading.Thread):
    def __init__(self, client_socket, finished_transaction, condition):
        super().__init__()
        self.client_socket = client_socket
        self.agency_id = None
        self.error = None
        self.finished_transaction = finished_transaction
        self.condition = condition
        self.winners_ready = threading.Event()
        self.winners = []

    def run(self):
        try:
            self.handle_client(self.client_socket)
        except Exception as e:
            self.error = e

    def mark_finished(self):
        with self.condition:
            self.finished_transaction[self.agency_id] = self
            self.condition.notify()

    def handle_header(self, header: bytes):
        action = "handle-header"
        message_type = header[0]
        length = int.from_bytes(header[1:5], byteorder="big")
        return message_type, length
    
    def handle_bets(self, client_socket, length):
        action = "handle-bet"
        client_message = safe_socket.recv_all(client_socket, length)
        return client_message.decode("utf-8")

    def handle_greetings(self, client_socket, length):
        action = "handle-greetings"
        logger.info(action, logger.LogResult.in_progress)
        client_message = safe_socket.recv_all(client_socket, length)
        return int.from_bytes(client_message, byteorder="big")

    def parse_bets(self, message: str, agency_id: int) -> list[Bet]:
        action = "parse-bets"
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
        csv_lines = []
        for bet in bets:
            csv_lines.append(
                f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}"
            )
        return "\n".join(csv_lines)

    def send_winners(self):
            action = "sending-winners"
            logger.info(action, logger.LogResult.in_progress,self.agency_id)
            acum = []
            for bet in self.winners:
                acum.append(bet)
                if len(acum) >= _CONN_BATCH:
                    winners_csv = self.bets_csv(acum)
                    message_length = len(winners_csv.encode("utf-8"))
                    header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
                    safe_socket.send_all(self.client_socket, header)
                    safe_socket.send_all(self.client_socket, winners_csv.encode("utf-8"))
                    acum = []
            if acum:
                winners_csv = self.bets_csv(acum)
                message_length = len(winners_csv.encode("utf-8"))
                header = bytes([_TYPE_WINNERS]) + message_length.to_bytes(4, byteorder="big")
                safe_socket.send_all(self.client_socket, header)
                safe_socket.send_all(self.client_socket, winners_csv.encode("utf-8"))
            header = bytes([_TYPE_END]) + (0).to_bytes(4, byteorder="big")
            safe_socket.send_all(self.client_socket, header)
            self.client_socket.close()

    def handle_client(self, client_socket) :
        action = "handle-client"
        message_amount = 0
        agency = -1
        length = 0
        lottery = Lottery("bets.csv")
        try:
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
                    self.mark_finished()
                    self.winners_ready.wait()
                    self.send_winners()
                    return
                                
            final_message = (f"Agency: {agency}, Messages received: {message_amount}\n")
            logger.info(action, logger.LogResult.success, final_message)
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

class Server:
    def __init__(self, server_host: str, server_port: int, agency_quorum_min: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agency_quorum_min = agency_quorum_min
        self.connections = []
        self.finished_transactions = {}
        self.condition = threading.Condition()

    def get_winners(self, finished_transactions, lottery: Lottery):
        action = "get-winners"
        logger.info(action, logger.LogResult.in_progress)
        winners = {}
        for bet in lottery.load_bets():
            if lottery.has_won(bet) and bet.agency_id in finished_transactions:
                if bet.agency_id not in winners:
                    winners[bet.agency_id] = [bet]
                else:
                    winners[bet.agency_id].append(bet)
        for w in winners:
            if winners[w]:
                finished_transactions[w].winners = winners[w]
        for agency_id in finished_transactions:
            connection = finished_transactions[agency_id]
            connection.winners_ready.set()

        

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            needed_conections = self.agency_quorum_min
            while True:
                while len(self.connections) < needed_conections:
                    try:
                        client_socket, _ = server_socket.accept()
                    except Exception as e:
                        logger.error(action, logger.LogResult.fail)
                        raise e
                    logger.info(action, logger.LogResult.success)
                    new_connection = Connection(client_socket, self.finished_transactions, self.condition)
                    self.connections.append(new_connection)
                    new_connection.start()

                with self.condition:
                    while len(self.finished_transactions) < self.agency_quorum_min:
                        self.condition.wait()
                    
                for c in self.connections:
                    if c.error is not None:
                        logger.info("Error en la transaccion de registros", c.error, [c.agency_id])
                if len(self.finished_transactions) >= self.agency_quorum_min:
                    #Aca se hace el sorteo
                    self.get_winners(self.finished_transactions, Lottery("bets.csv"))
                    for connection in self.finished_transactions.values():
                        connection.join()
                    self.finished_transactions = {}
                    needed_conections = self.agency_quorum_min
                else:
                    needed_conections -= len(self.finished_transactions)
                self.connections = []
