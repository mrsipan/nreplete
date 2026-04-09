import collections
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
        def error_cnx():
            logger.error('socket disconnected')
            raise Exception('socket disconnected')

        while not self.stop_reader_event.is_set():
            chk = chk if (chk := self.socket_connection.recv(4096)
                         ) else error_cnx()
            self.receive_buffer.extend(chk)

            # Attempt to extract complete messages from the buffer
            while True:
                try:
                    decoded_message, bytes_consumed = self._decode_1_bencode(
                        self.receive_buffer
                        )
                except Exception:
                    break
                self.receive_buffer = self.receive_buffer[
                    bytes_consumed:]
                # Convert all bytes in the message to UTF-8 strings
                message = self._bytes_to_strings(decoded_message)
                self.received_messages_queue.put(message)
                logger.debug(f"Received message: {message}")

        logger.info("Reader loop terminated.")

    @staticmethod
    def _decode_1_bencode(buffer: bytes):
        """
        Parse a single bencode message from the beginning of a bytes buffer.
        Returns a tuple (decoded_object, bytes_consumed).
        Raises ValueError if the buffer does not contain a complete valid message.
        """
        idx = 0
        buffer_len = len(buffer)

        def parse():
            nonlocal idx
            if idx >= buffer_len:
                raise ValueError("Incomplete data")

            token = buffer[idx]
            idx += 1

            if token == ord('i'):  # integer: i<number>e
                start = idx
                while idx < buffer_len and buffer[idx] != ord('e'):
                    idx += 1
                if idx >= buffer_len:
                    raise ValueError("Incomplete integer")
                number_string = buffer[start:idx].decode('utf-8')
                idx += 1  # consume 'e'
                return int(number_string)

            elif token == ord('l'):  # list: l...e
                result = []
                while idx < buffer_len and buffer[idx] != ord('e'):
                    result.append(parse())
                if idx >= buffer_len:
                    raise ValueError("Incomplete list")
                idx += 1  # consume 'e'
                return result

            elif token == ord('d'):  # dictionary: d...e
                result = {}
                while idx < buffer_len and buffer[idx] != ord('e'):
                    key = parse()
                    if not isinstance(key, bytes):
                        raise ValueError(
                            "Dictionary key must be a bencode string"
                            )
                    value = parse()
                    result[key] = value
                if idx >= buffer_len:
                    raise ValueError("Incomplete dictionary")
                idx += 1  # consume 'e'
                return result

            elif 48 <= token <= 57:  # ASCII digit: string: <length>:<data>
                # token is the first digit; we need the full length string
                start = idx - 1  # include the digit we already consumed
                while idx < buffer_len and buffer[idx] != ord(':'):
                    idx += 1
                if idx >= buffer_len:
                    raise ValueError("Incomplete string length")
                length_str = buffer[start:idx].decode('utf-8')
                idx += 1  # consume ':'
                length = int(length_str)
                if idx + length > buffer_len:
                    raise ValueError("Incomplete string data")
                # data = buffer[idx:idx + length]
                data = bytes(buffer[idx:idx + length])
                idx += length
                return data

            else:
                raise ValueError(f"Invalid bencode token: {chr(token)}")

        try:
            obj = parse()
        except ValueError as err:
            # Re-raise with the original context
            raise ValueError(f"Failed to decode bencode: {err}")
        else:
            return obj, idx

    @staticmethod
    def _bytes_to_strings(obj):
        """Recursively convert all bytes objects in a decoded structure to UTF-8 strings."""
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
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
        msg_bytes = self._strings_to_bytes(message)
        data_to_send = bencode2.bencode(msg_bytes)

        try:
            self.socket_connection.sendall(data_to_send)
            logger.debug(f"Sent message: {message}")
        except socket.error as send_error:
            logger.error(f"Failed to send message: {send_error}")
            raise

    def send_and_wait_for_response(
        self, message, response_id=None, timeout=30.0
        ):
        self.send_message(message)
        d1 = {'out': [''], 'err': [''], 'value': ['nil']}
        acc = collections.defaultdict(list, d1)

        while True:
            try:
                msg = self.received_messages_queue.get(timeout=timeout)
            except queue.Empty:
                # Failsafe if the browser is closed or network drops
                print('empty queue')
                return acc

            print(msg)

            if msg.get('id') == response_id:
                # Stream output live
                if 'out' in msg:
                    acc['out'].append(msg['out'])

                if 'err' in msg:
                    acc['err'].append(msg['err'])

                # Store the value
                if 'value' in msg:
                    acc['value'].append(msg['value'])

                if 'ex' in msg and msg['ex']:
                    # acc['ex'] = msg['ex']
                    return {
                        key: '\n'.join(value)
                        for key, value in acc.items()
                        } | {
                            'ex': msg['ex']
                            }

                if 'status' in msg and 'done' in msg['status']:
                    return {
                        key: '\n'.join(value)
                        for key, value in acc.items()
                        }

    def evaluate(
        self,
        code: str,
        session_id: str = None,
        namespace: str = None,
        file_name: str = None,
        line_number: int = None,
        column_number: int = None,
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
        print('starting')
        client.connect()
        # description = client.describe()
        # print("Server description:", description)

        # result = client.evaluate("(+ 1 2 3)")
        # print("Evaluation result:", result)

        # result = client.evaluate(
        #     "(println \"Hello from nREPL client!\")"
        #     )
        # print("Evaluation result:", result)

        # client.evaluate(
        #     '''
        #     (require '[squint.string :as str])
        #         (defn ip-to-int [ip-str]
        # (let [parts (str/split ip-str ".")
        # [a b c d] (map #(js/parseInt % 10) parts)]
        # (+ (* a 256 256 256) (* b 256 256) (* c 256) d)))

        #         '''
        #     )

        # print(
        #     client.evaluate(
        #         """
        # (ns app.core)

        # ;; IP calculation utilities
        # (defn ip-to-int [ip-str]
        # (let [parts (str/split ip-str ".")
        # [a b c d] (map #(js/parseInt % 10) parts)]
        # (+ (* a 256 256 256) (* b 256 256) (* c 256) d)))

        # (defn int-to-ip [n]
        # (let [a (bit-and (bit-shift-right n 24) 0xFF)
        # b (bit-and (bit-shift-right n 16) 0xFF)
        # c (bit-and (bit-shift-right n 8) 0xFF)
        # d (bit-and n 0xFF)]
        # (str a "." b "." c "." d)))

        # (defn cidr-to-mask [cidr]
        # (let [mask (- (bit-shift-left 1 32) 1)
        # network-mask (bit-shift-left mask (- 32 cidr))]
        # network-mask))

        # (defn mask-to-cidr [mask-int]
        # (let [bits (bit-count mask-int)]
        # bits))

        # ;; Test
        # (prn "Testing IP conversion:" (ip-to-int "192.168.1.1"))
        # (prn "Int to IP:" (int-to-ip 3232235777))
        # (prn "CIDR 24 mask:" (int-to-ip (cidr-to-mask 24)))
        # (prn "Mask to CIDR:" (mask-to-cidr (cidr-to-mask 24)))

        #             """
        #         )
        #     )

        print('otro')
        print(
            client.evaluate(
                '''
                (require '["https://unpkg.com/squint-cljs/src/squint/string.js" :as str])
                (str/split "clojure ,another|    squint js" #",")
                ;(.split "clojure another r| squint js" "|")
                (prn {:a 2 :b 3})

                '''
                )
            )
        # print(
        #     client.evaluate(
        #         """
        #     ;; 1. Run the require
        #     ;(require '["https://unpkg.com/squint-cljs/src/squint/string.js" :as str])

        #     (str/upper-case "hola que tal")
        #     (str/lower-case "WEEEE")

        #     ;; 2. Use the alias 'str'
        #     ;;(str/split "clojure,squint,js" #",")
        #     ;; => ["clojure" "squint" "js"]

        #     (println (str/upper_case "hello"))
        #     ;; => "HELLO"

        #     """
        #         )
        #     )
        # client.evaluate(
        #     """(require '[clojure.string :as ww]) (ww/split "ddd a as df" #" ")"""
        #     )

        # print('otro mas')
        # print(client.evaluate('(ww/split "holas que tal" #" ")'))
        # print(client.evaluate('(ww/split "asdf www eee rr" #" ")'))
        print(client.evaluate('(defn f [x] (* x x))'))
        print(client.evaluate('(println (f 9))'))

    except Exception as err:
        logger.exception(err)
        print(f"Error: {err}")

    finally:
        client.close()
