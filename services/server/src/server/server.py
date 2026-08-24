import socket
import logger
import safe_socket

_ECHO_SERVER_MESSAGE_SIZE = 1024


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency = ""
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                client_message = safe_socket.recv_all( client_socket, _ECHO_SERVER_MESSAGE_SIZE)
                if not client_message:
                    break

                if agency is None:
                    agency = client_message.decode("utf-8").strip()
                    continue
                
                if client_message == b"EOF\n":
                    logger.info(
                        action,
                        logger.LogResult.success,
                        "messages-amount",
                        message_amount,
                    )
                    break
                message_amount += 1
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
