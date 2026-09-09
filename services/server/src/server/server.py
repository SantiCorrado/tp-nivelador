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
_TYPE_ACK = 5

BETS_FILE = "bets.csv"

# Esta clase representa una conexion con un cliente (agencia) y se encarga de recibir las apuestas, procesarlas y enviar los ganadores cuando el thread principal lo indique
class Connection(threading.Thread):
    def __init__(self, client_socket, finished_transaction, condition, sigtermarrived, lottery, lottery_lock):
        super().__init__()
        self.client_socket = client_socket
        self.agency_id = None
        self.error = None
        self.condition = condition #Esta condition se utiliza para notificar al thread principal cuando una agencia termina de enviar sus apuestas o falla
        self.finished_transaction = finished_transaction
        self.winners_ready = threading.Event() #Esta event se utiliza por el thread principal para notificar a este thread que ya puede enviar los ganadores al cliente
        self.winners = [] # Una vez que se ejecuta winners_ready, se cargan los ganadores de la agencia en esta lista y se envian al cliente
        self.sigtermarrived = sigtermarrived
        self.lottery = lottery
        self.lottery_lock = lottery_lock

    def run(self):
        try:
            self.handle_client(self.client_socket)
        except Exception as e:
            if not self.sigtermarrived.is_set():
                with self.condition:
                    self.finished_transaction[self.agency_id] = self
                    self.condition.notify()
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
        winners_csv = self.bets_csv(self.winners)
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
        try:
            while not self.sigtermarrived.is_set():
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
                    with self.lottery_lock:
                        self.lottery.store_bets(bets)
                    message_amount += 1
                    ack_header = bytes([_TYPE_ACK]) + (0).to_bytes(4, byteorder="big")
                    safe_socket.send_all(client_socket, ack_header)
                elif message_type == _TYPE_END:
                    self.mark_finished()
                    while not self.sigtermarrived.is_set():
                        if self.winners_ready.wait(timeout=1):
                            self.send_winners()
                            return
                    return
            if self.sigtermarrived.is_set():
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
        self.finished_transactions = {}# Diccionario de threads que ya registraron las apuestas de su cliente pero aun no realizaron el sorteo de ganadores
        self.condition = threading.Condition()
        self.lottery = Lottery(BETS_FILE)
        self.lottery_lock = threading.Lock()

    #Aca se obtiene la lista de ganadores de cada agencia y se les notifica a cada thread del servidor
    #   que ya pueden enviar los ganadores al cliente
    def get_winners(self, finished_transactions):
        action = "get-winners"
        logger.info(action, logger.LogResult.in_progress)
        winners = {}
        with self.lottery_lock:
            for bet in self.lottery.load_bets():
                if self.lottery.has_won(bet) and bet.agency_id in finished_transactions:
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

    def close_connections(self):
        for connection in self.connections:
            connection.client_socket.close()
        for connection in self.connections:
            connection.join(timeout=1)
        

    def run(self,sigterm_arrived):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            server_socket.settimeout(1)
            needed_conections = self.agency_quorum_min
            while not sigterm_arrived.is_set():
                # Aca se aceptan conexiones hasta que se llegue a la cantidad minima de conexiones requeridas
                while len(self.connections) < needed_conections and not sigterm_arrived.is_set():
                    try:
                        client_socket, _ = server_socket.accept()
                    except socket.timeout:
                        continue
                    except Exception as e:
                        if sigterm_arrived.is_set():
                            logger.info("sigterm-handler", logger.LogResult.success, "SIGTERM received")
                            break
                        logger.error(action, logger.LogResult.fail)
                        raise e
                    logger.info(action, logger.LogResult.success)
                    new_connection = Connection(client_socket, self.finished_transactions, self.condition, sigterm_arrived, self.lottery, self.lottery_lock)
                    self.connections.append(new_connection)
                    new_connection.start()

                #Aca se espera a que todas las conexiones terminen de enviar sus apuestas (o a que fallen)
                with self.condition:
                    while len(self.finished_transactions) < self.agency_quorum_min and not sigterm_arrived.is_set():
                        self.condition.wait(timeout=1)
                successful_connections = {}
                #Aca se filtran las conexiones succesful de las que fallaron
                for agency_id in self.finished_transactions:
                    if self.finished_transactions[agency_id].error is None:
                        successful_connections[agency_id] = self.finished_transactions[agency_id]
                    else:
                        logger.info("Error en la transaccion de registros", self.finished_transactions[agency_id].error, [agency_id])
                #En el caso de que se tenga una cantidad de conexiones succesful mayor o igual a la cantidad minima requerida, se hace el sorteo y se envian los ganadores a cada agencia
                if len(successful_connections) == self.agency_quorum_min and not sigterm_arrived.is_set():
                    #Aca se hace el sorteo y se envian los ganadores a cada agencia
                    self.get_winners(successful_connections)
                    for connection in successful_connections.values():
                        #Aca se espera a que cada thread de conexion envie los ganadores al cliente y finalice su ejecucion
                        connection.join(timeout=1)
                    #Aca se reinician las conexiones y se vuelve a esperar a que todas las conexiones envien sus ganadores
                    self.finished_transactions = {}
                    self.connections = []
                    needed_conections = self.agency_quorum_min
                else:
                    #En el caso de que no se tenga la cantidad minima de conexiones succesful, en la proxima iteracion
                    #   solo se aceptaran la cantidad de conexiones que falten para llegar a la cantidad minima requerida
                    #   y se mantiene la conexion con las agencias que ya enviaron sus apuestas succesfully
                    needed_conections = self.agency_quorum_min - len(successful_connections)

                if sigterm_arrived.is_set():
                    logger.info("sigterm-handler", logger.LogResult.success, "SIGTERM received")
                    break
            self.close_connections()
