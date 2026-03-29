import logging
import queue
import socket
import threading
import time

import bencode2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NreplClient:
    """
    A client for interacting with an nREPL server using the bencode2 library.
    """
    def __init__(
        self, host: str, port: int, connect_timeout: float = 5.0
        ):
        """
        Initialize the client with connection parameters.

        :param host: Server hostname or IP address.
        :param port: Server port.
        :param connect_timeout: Timeout for socket connection in seconds.
        """
        self.host = host
        self.port = port
        self.connect_timeout = connect_timeout

        self.socket_connection = None
        self.reader_thread = None
        self.stop_reader_event = threading.Event()
        self.received_messages_queue = queue.Queue()
        self.receive_buffer = bytearray()

        self.current_session_id = None

    def connect(self) -> None:
        """Establish a TCP connection to the nREPL server and start the reader thread."""
        logger.info(
            f"Connecting to nREPL server at {self.host}:{self.port}"
            )
        self.socket_connection = socket.socket(
            socket.AF_INET, socket.SOCK_STREAM
            )
        self.socket_connection.settimeout(self.connect_timeout)

        try:
            self.socket_connection.connect((self.host, self.port))
            logger.info("Socket connection established.")
        except Exception as connection_error:
            logger.error(f"Failed to connect: {connection_error}")
            raise

        self.reader_thread = threading.Thread(
            target=self._reader_loop, daemon=True
            )
        self.reader_thread.start()
        logger.debug("Reader thread started.")

    def close(self) -> None:
        """Close the socket and stop the reader thread."""
        if self.socket_connection:
            logger.info("Closing socket connection.")
            self.stop_reader_event.set()
            self.socket_connection.close()
            self.socket_connection = None
        if self.reader_thread and self.reader_thread.is_alive():
            self.reader_thread.join(timeout=1.0)
            logger.debug("Reader thread stopped.")

    def _reader_loop(self) -> None:
        """
        Background thread: reads data, extracts complete bencode messages,
        converts bytes to strings, and puts them into the queue.
        """
        while not self.stop_reader_event.is_set():
            try:
                chunk = self.socket_connection.recv(4096)
                if not chunk:
                    logger.warning("Socket connection closed by peer.")
                    break
                self.receive_buffer.extend(chunk)

                # Attempt to extract complete messages from the buffer
                while True:
                    try:
                        decoded_message, bytes_consumed = self._decode_one_bencode(
                            self.receive_buffer
                            )
                        # Remove the consumed bytes from the buffer
                        self.receive_buffer = self.receive_buffer[
                            bytes_consumed:]
                        # Convert all bytes in the message to UTF-8 strings
                        message = self._bytes_to_strings(
                            decoded_message
                            )
                        self.received_messages_queue.put(message)
                        logger.debug(f"Received message: {message}")
                    except ValueError as parse_error:
                        # Not enough data or invalid message; wait for more
                        break
            except socket.timeout:
                continue
            except socket.error as socket_error:
                logger.error(
                    f"Socket error in reader loop: {socket_error}"
                    )
                break
            except Exception as unexpected_error:
                logger.exception(
                    f"Unexpected error in reader loop: {unexpected_error}"
                    )
                break

        logger.info("Reader loop terminated.")

    @staticmethod
    def _decode_one_bencode(buffer: bytes):
        """
        Parse a single bencode message from the beginning of a bytes buffer.
        Returns a tuple (decoded_object, bytes_consumed).
        Raises ValueError if the buffer does not contain a complete valid message.
        """
        index = 0
        buffer_len = len(buffer)

        def parse():
            nonlocal index
            if index >= buffer_len:
                raise ValueError("Incomplete data")

            token = buffer[index]
            index += 1

            if token == ord('i'):  # integer: i<number>e
                start = index
                while index < buffer_len and buffer[index] != ord('e'):
                    index += 1
                if index >= buffer_len:
                    raise ValueError("Incomplete integer")
                number_str = buffer[start:index].decode('utf-8')
                index += 1  # consume 'e'
                return int(number_str)

            elif token == ord('l'):  # list: l...e
                result = []
                while index < buffer_len and buffer[index] != ord('e'):
                    result.append(parse())
                if index >= buffer_len:
                    raise ValueError("Incomplete list")
                index += 1  # consume 'e'
                return result

            elif token == ord('d'):  # dictionary: d...e
                result = {}
                while index < buffer_len and buffer[index] != ord('e'):
                    key = parse()
                    if not isinstance(key, bytes):
                        raise ValueError(
                            "Dictionary key must be a bencode string"
                            )
                    value = parse()
                    result[key] = value
                if index >= buffer_len:
                    raise ValueError("Incomplete dictionary")
                index += 1  # consume 'e'
                return result

            elif 48 <= token <= 57:  # ASCII digit: string: <length>:<data>
                # token is the first digit; we need the full length string
                start = index - 1  # include the digit we already consumed
                while index < buffer_len and buffer[index] != ord(':'):
                    index += 1
                if index >= buffer_len:
                    raise ValueError("Incomplete string length")
                length_str = buffer[start:index].decode('utf-8')
                index += 1  # consume ':'
                length = int(length_str)
                if index + length > buffer_len:
                    raise ValueError("Incomplete string data")
                # data = buffer[index:index + length]
                data = bytes(buffer[index:index + length])
                index += length
                return data

            else:
                raise ValueError(f"Invalid bencode token: {chr(token)}")

        try:
            obj = parse()
            return obj, index
        except ValueError as err:
            # Re-raise with the original context
            raise ValueError(f"Failed to decode bencode: {err}")

    @staticmethod
    def _bytes_to_strings(obj):
        """Recursively convert all bytes objects in a decoded structure to UTF-8 strings."""
        if isinstance(obj, bytes):
            return obj.decode('utf-8')
        elif isinstance(obj, dict):
            return {
                NreplClient._bytes_to_strings(k):
                    NreplClient._bytes_to_strings(v)
                for k, v in obj.items()
                }
        elif isinstance(obj, (list, tuple)):
            return [NreplClient._bytes_to_strings(item) for item in obj]
        else:
            return obj

    @staticmethod
    def _strings_to_bytes(obj):
        """Recursively convert all string objects in a message to bytes for bencode encoding."""
        if isinstance(obj, str):
            return obj.encode('utf-8')
        elif isinstance(obj, dict):
            return {
                NreplClient._strings_to_bytes(k):
                    NreplClient._strings_to_bytes(v)
                for k, v in obj.items()
                }
        elif isinstance(obj, (list, tuple)):
            return [NreplClient._strings_to_bytes(item) for item in obj]
        else:
            return obj

    def send_message(self, message: dict) -> None:
        """Send a bencode-encoded message to the server."""
        if not self.socket_connection:
            raise RuntimeError(
                "Not connected to server. Call connect() first."
                )

        # Convert strings to bytes (bencode2 can handle strings, but we do it for consistency)
        message_bytes = self._strings_to_bytes(message)
        data_to_send = bencode2.bencode(message_bytes)

        try:
            self.socket_connection.sendall(data_to_send)
            logger.debug(f"Sent message: {message}")
        except socket.error as send_error:
            logger.error(f"Failed to send message: {send_error}")
            raise

    def send_and_wait_for_response(
        self,
        message: dict,
        response_id: str = None,
        timeout: float = 10.0
        ) -> dict:
        """
        Send a message and wait for all responses until status contains 'done'.
        Returns a merged dict of all received messages for this request.
        """
        if response_id is None:
            response_id = message.get('id')
        if response_id is None:
            raise ValueError(
                "Message must contain an 'id' field, or provide response_id."
                )
        self.send_message(message)
        accumulated = {}
        while True:
            received = self.received_messages_queue.get(timeout=timeout)
            if received.get('id') == response_id:
                # Merge into accumulated, collecting 'out'/'err' as lists
                for k, v in received.items():
                    if k in ('out', 'err') and k in accumulated:
                        accumulated[
                            k
                            ] += v  # concatenate successive out/err chunks
                    else:
                        accumulated[k] = v
                if 'done' in received.get('status', []):
                    return accumulated

    def evaluate(
        self,
        code: str,
        session_id: str = None,
        namespace: str = None,
        file_name: str = None,
        line_number: int = None,
        column_number: int = None
        ) -> dict:
        """
        Send an 'eval' operation and wait for the result.
        """
        request_id = f"req-{int(time.time() * 1000)}"

        message = {"op": "eval", "code": code, "id": request_id}

        actual_session = session_id or self.current_session_id
        if actual_session:
            message["session"] = actual_session
        if namespace:
            message["ns"] = namespace
        if file_name:
            message["file"] = file_name
        if line_number is not None:
            message["line"] = line_number
        if column_number is not None:
            message["column"] = column_number

        response = self.send_and_wait_for_response(
            message, response_id=request_id
            )

        # If the response contains a new session (first message after connect), store it
        if 'session' in response and not self.current_session_id:
            self.current_session_id = response['session']
            logger.info(
                f"Stored new session: {self.current_session_id}"
                )

        return response

    def describe(self) -> dict:
        """Send the 'describe' operation to get server capabilities."""
        request_id = f"desc-{int(time.time() * 1000)}"
        message = {"op": "describe", "id": request_id}
        return self.send_and_wait_for_response(
            message, response_id=request_id
            )


# Example usage
if __name__ == "__main__":
    client = NreplClient("localhost", 7888)

    try:
        client.connect()
        description = client.describe()
        print("Server description:", description)

        result = client.evaluate("(+ 1 2 3)")
        print("Evaluation result:", result)

        result = client.evaluate(
            "(println \"Hello from nREPL client!\")"
            )
        print("Evaluation result:", result)

    except Exception as err:
        logger.exception(err)
        print(f"Error: {err}")

    finally:
        client.close()
