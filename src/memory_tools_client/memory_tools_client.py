import asyncio
import ssl
import json
import struct
import base64
from typing import Any, Dict, List, Optional
import logging

# Configure logging for debug messages
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Protocol Constants (must match internal/protocol/protocol.go) ---
# COMMAND TYPES
CMD_SET = 1
CMD_GET = 2
CMD_COLLECTION_CREATE = 3
CMD_COLLECTION_DELETE = 4
CMD_COLLECTION_LIST = 5
CMD_COLLECTION_ITEM_SET = 6
CMD_COLLECTION_ITEM_SET_MANY = 7
CMD_COLLECTION_ITEM_GET = 8
CMD_COLLECTION_ITEM_DELETE = 9
CMD_COLLECTION_ITEM_LIST = 10
CMD_COLLECTION_QUERY = 11
CMD_COLLECTION_ITEM_DELETE_MANY = 12
CMD_AUTHENTICATE = 13

# RESPONSE STATUS
STATUS_OK = 1
STATUS_NOT_FOUND = 2
STATUS_ERROR = 3
STATUS_BAD_COMMAND = 4
STATUS_UNAUTHORIZED = 5
STATUS_BAD_REQUEST = 6


# Helper function to get status string for better error messages
def get_status_string(status: int) -> str:
    """Returns the status string for a given status number."""
    if status == STATUS_OK:
        return "OK"
    if status == STATUS_NOT_FOUND:
        return "NOT_FOUND"
    if status == STATUS_ERROR:
        return "ERROR"
    if status == STATUS_BAD_COMMAND:
        return "BAD_COMMAND"
    if status == STATUS_UNAUTHORIZED:
        return "UNAUTHORIZED"
    if status == STATUS_BAD_REQUEST:
        return "BAD_REQUEST"
    return "UNKNOWN_STATUS"


# Classes to replicate TypeScript interfaces
class CommandResponse:
    def __init__(self, status: int, message: str, data: bytes):
        self.status = status
        self.message = message
        self.data = data


class GetResult:
    def __init__(self, found: bool, message: str, value: Optional[Any]):
        self.found = found
        self.message = message
        self.value = value


class CollectionListResult:
    def __init__(self, message: str, names: List[str]):
        self.message = message
        self.names = names


class CollectionItemListResult:
    def __init__(self, message: str, items: Dict[str, Any]):
        self.message = message
        self.items = items


class OrderByClause:
    def __init__(self, field: str, direction: str):
        self.field = field
        self.direction = direction


class Aggregation:
    def __init__(self, func: str, field: str):
        self.func = func
        self.field = field


class Query:
    def __init__(
        self,
        filter: Optional[Dict[str, Any]] = None,
        orderBy: Optional[List[OrderByClause]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        count: Optional[bool] = None,
        aggregations: Optional[Dict[str, Aggregation]] = None,
        groupBy: Optional[List[str]] = None,
        having: Optional[Dict[str, Any]] = None,
        distinct: Optional[str] = None,
    ):
        self.filter = filter
        self.orderBy = orderBy
        self.limit = limit
        self.offset = offset
        self.count = count
        self.aggregations = aggregations
        self.groupBy = groupBy
        self.having = having
        self.distinct = distinct

    def to_json(self) -> bytes:
        # Convert the Query object to a dictionary for JSON
        data = {
            "filter": self.filter,
            "orderBy": [o.__dict__ for o in self.orderBy] if self.orderBy else None,
            "limit": self.limit,
            "offset": self.offset,
            "count": self.count,
            "aggregations": (
                {k: v.__dict__ for k, v in self.aggregations.items()}
                if self.aggregations
                else None
            ),
            "groupBy": self.groupBy,
            "having": self.having,
            "distinct": self.distinct,
        }
        # Clean up keys with None values so they are not sent
        return json.dumps({k: v for k, v in data.items() if v is not None}).encode(
            "utf-8"
        )


# --- Helper Functions ---


def write_string(s: str) -> bytes:
    """Writes a length-prefixed string (uint32 Little Endian)."""
    s_bytes = s.encode("utf-8")
    len_bytes = struct.pack("<L", len(s_bytes))
    return len_bytes + s_bytes


def write_bytes(b: bytes) -> bytes:
    """Writes a length-prefixed byte array (uint32 Little Endian)."""
    len_bytes = struct.pack("<L", len(b))
    return len_bytes + b


async def read_n_bytes(reader: asyncio.StreamReader, n: int) -> bytes:
    """Reads N bytes from the socket, handling partial reads."""
    data = await reader.readexactly(n)
    return data


# --- DB Client Class ---
class MemoryToolsClient:
    def __init__(
        self,
        host: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        server_cert_path: Optional[str] = None,
        reject_unauthorized: bool = True,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.server_cert_path = server_cert_path
        self.reject_unauthorized = reject_unauthorized
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.is_authenticated_session = False
        self.authenticated_user: Optional[str] = None
        self.connecting_task: Optional[asyncio.Task] = None

    async def connect(self):
        """Establishes a TLS connection to the database server and performs authentication."""
        # If already connected and authenticated, just return.
        if self.reader and not self.reader.at_eof() and self.is_authenticated_session:
            logging.info("DBClient: Already connected and authenticated.")
            return

        # If a connection attempt is already in progress, wait for it to finish.
        if self.connecting_task:
            await self.connecting_task
            return

        async def _do_connect():
            ssl_context = ssl.create_default_context(
                cafile=self.server_cert_path if self.server_cert_path else None
            )
            ssl_context.check_hostname = self.reject_unauthorized
            ssl_context.verify_mode = (
                ssl.CERT_REQUIRED if self.reject_unauthorized else ssl.CERT_NONE
            )

            try:
                reader, writer = await asyncio.open_connection(
                    self.host, self.port, ssl=ssl_context
                )
                self.reader, self.writer = reader, writer
                logging.info(f"DBClient: Connected securely to {self.host}:{self.port}")

                if self.username and self.password:
                    # Perform authentication automatically
                    await self._perform_authentication(self.username, self.password)
                else:
                    logging.warning(
                        "DBClient: Connected without credentials. Operations might be unauthorized."
                    )

            except Exception as e:
                logging.error(f"DBClient: Connection error: {e}")
                self.close()
                raise e

        self.connecting_task = asyncio.create_task(_do_connect())
        await self.connecting_task
        self.connecting_task = None

    async def _perform_authentication(self, username: str, password: str) -> str:
        """Internal method to perform the authentication command."""
        if not self.writer:
            raise Exception(
                "DBClient: Cannot perform authentication, socket is not connected."
            )

        username_buffer = write_string(username)
        password_buffer = write_string(password)
        payload = username_buffer + password_buffer

        command_buffer = bytes([CMD_AUTHENTICATE]) + payload
        self.writer.write(command_buffer)
        await self.writer.drain()

        # Read response (status, message, data)
        status, message, data = await self._read_response()

        if status == STATUS_OK:
            self.is_authenticated_session = True
            self.authenticated_user = username
            logging.info(f"DBClient: Authentication successful for user '{username}'.")
            return message
        else:
            self.is_authenticated_session = False
            self.authenticated_user = None
            error_message = (
                f"Authentication failed: {message}"
                if status == STATUS_UNAUTHORIZED
                else f"Authentication failed with status {get_status_string(status)} ({status}): {message}"
            )
            raise Exception(error_message)

    async def _read_response(self) -> tuple[int, str, bytes]:
        """Reads a full response from the server (status, message, data)."""
        if not self.reader:
            raise Exception("DBClient: Cannot read response, socket is not connected.")

        status_byte = await read_n_bytes(self.reader, 1)
        status = status_byte[0]

        msg_len_buffer = await read_n_bytes(self.reader, 4)
        msg_len = struct.unpack("<L", msg_len_buffer)[0]
        msg_buffer = await read_n_bytes(self.reader, msg_len)
        message = msg_buffer.decode("utf-8")

        data_len_buffer = await read_n_bytes(self.reader, 4)
        data_len = struct.unpack("<L", data_len_buffer)[0]
        data_buffer = await read_n_bytes(self.reader, data_len) if data_len > 0 else b""

        return status, message, data_buffer

    async def _send_command(
        self, command_type: int, payload_buffer: bytes
    ) -> CommandResponse:
        """Sends a command and reads its response from the server."""
        await self.connect()

        if not self.writer:
            raise Exception("DBClient: Not connected. Call connect() first.")

        # Ensure the session is authenticated for any command other than CMD_AUTHENTICATE
        if command_type != CMD_AUTHENTICATE and not self.is_authenticated_session:
            raise Exception(
                "DBClient: Not authenticated. Call connect() with credentials to authenticate."
            )

        command_buffer = bytes([command_type]) + payload_buffer
        self.writer.write(command_buffer)
        await self.writer.drain()

        status, message, data = await self._read_response()
        return CommandResponse(status, message, data)

    # --- Public API Methods ---

    async def set(self, key: str, value: Any, ttl_seconds: int = 0) -> str:
        """Sets a key-value pair in the main store."""
        key_buffer = write_string(key)
        value_buffer = write_bytes(json.dumps(value).encode("utf-8"))
        ttl_buffer = struct.pack("<Q", ttl_seconds)  # uint64 Little Endian
        payload = key_buffer + value_buffer + ttl_buffer
        response = await self._send_command(CMD_SET, payload)

        if response.status != STATUS_OK:
            raise Exception(
                f"SET failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def get(self, key: str) -> GetResult:
        """Retrieves a value from the main store by key."""
        key_buffer = write_string(key)
        payload = key_buffer
        response = await self._send_command(CMD_GET, payload)

        if response.status == STATUS_NOT_FOUND:
            return GetResult(found=False, message=response.message, value=None)
        if response.status != STATUS_OK:
            raise Exception(
                f"GET failed: {get_status_string(response.status)}: {response.message}"
            )

        try:
            value = json.loads(response.data.decode("utf-8"))
            return GetResult(found=True, message=response.message, value=value)
        except json.JSONDecodeError as e:
            logging.error(
                f"DBClient: Error parsing GET value as JSON: {e}. Raw: {response.data.decode('utf-8')}"
            )
            raise Exception(f"GET failed: Invalid JSON format for stored value.")

    async def collection_create(self, collection_name: str) -> str:
        """Ensures a collection exists (creates it if it doesn't)."""
        payload = write_string(collection_name)
        response = await self._send_command(CMD_COLLECTION_CREATE, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_CREATE failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_delete(self, collection_name: str) -> str:
        """Deletes a collection entirely."""
        payload = write_string(collection_name)
        response = await self._send_command(CMD_COLLECTION_DELETE, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_DELETE failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_list(self) -> CollectionListResult:
        """Lists all available collection names."""
        payload = b""
        response = await self._send_command(CMD_COLLECTION_LIST, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_LIST failed: {get_status_string(response.status)}: {response.message}"
            )
        try:
            names = json.loads(response.data.decode("utf-8"))
            return CollectionListResult(message=response.message, names=names)
        except json.JSONDecodeError as e:
            logging.error(
                f"DBClient: Error parsing COLLECTION_LIST response as JSON: {e}. Raw: {response.data.decode('utf-8')}"
            )
            raise Exception(
                f"COLLECTION_LIST failed: Invalid JSON format for collection names."
            )

    async def collection_item_set(
        self, collection_name: str, key: str, value: Any, ttl_seconds: int = 0
    ) -> str:
        """Sets an item (key-value pair) within a specific collection."""
        collection_name_buffer = write_string(collection_name)
        key_buffer = write_string(key)
        value_buffer = write_bytes(json.dumps(value).encode("utf-8"))
        ttl_buffer = struct.pack("<Q", ttl_seconds)
        payload = collection_name_buffer + key_buffer + value_buffer + ttl_buffer
        response = await self._send_command(CMD_COLLECTION_ITEM_SET, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_SET failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_item_set_many(
        self, collection_name: str, values: List[Any]
    ) -> str:
        """Sets multiple items in a collection from a JSON array."""
        collection_name_buffer = write_string(collection_name)
        values_json_bytes = json.dumps(values).encode("utf-8")
        values_buffer = write_bytes(values_json_bytes)
        payload = collection_name_buffer + values_buffer
        response = await self._send_command(CMD_COLLECTION_ITEM_SET_MANY, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_SET_MANY failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_item_get(self, collection_name: str, key: str) -> GetResult:
        """Retrieves an item from a specific collection by key."""
        collection_name_buffer = write_string(collection_name)
        key_buffer = write_string(key)
        payload = collection_name_buffer + key_buffer
        response = await self._send_command(CMD_COLLECTION_ITEM_GET, payload)

        if response.status == STATUS_NOT_FOUND:
            return GetResult(found=False, message=response.message, value=None)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_GET failed: {get_status_string(response.status)}: {response.message}"
            )

        try:
            value = json.loads(response.data.decode("utf-8"))
            return GetResult(found=True, message=response.message, value=value)
        except json.JSONDecodeError as e:
            logging.error(
                f"DBClient: Error parsing COLLECTION_ITEM_GET value as JSON: {e}. Raw: {response.data.decode('utf-8')}"
            )
            raise Exception(
                f"COLLECTION_ITEM_GET failed: Invalid JSON format for stored value."
            )

    async def collection_item_delete(self, collection_name: str, key: str) -> str:
        """Deletes an item from a specific collection by key."""
        collection_name_buffer = write_string(collection_name)
        key_buffer = write_string(key)
        payload = collection_name_buffer + key_buffer
        response = await self._send_command(CMD_COLLECTION_ITEM_DELETE, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_DELETE failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_item_delete_many(
        self, collection_name: str, keys: List[str]
    ) -> str:
        """Deletes multiple items from a collection by their keys."""
        collection_name_buffer = write_string(collection_name)
        keys_count_buffer = struct.pack("<L", len(keys))
        keys_payload = b"".join(write_string(key) for key in keys)
        payload = collection_name_buffer + keys_count_buffer + keys_payload
        response = await self._send_command(CMD_COLLECTION_ITEM_DELETE_MANY, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_DELETE_MANY failed: {get_status_string(response.status)}: {response.message}"
            )
        return response.message

    async def collection_item_list(
        self, collection_name: str
    ) -> CollectionItemListResult:
        """Lists all items (key-value pairs) within a specific collection."""
        payload = write_string(collection_name)
        response = await self._send_command(CMD_COLLECTION_ITEM_LIST, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_ITEM_LIST failed: {get_status_string(response.status)}: {response.message}"
            )

        raw_map = json.loads(response.data.decode("utf-8"))
        decoded_map = {}
        for key, raw_val in raw_map.items():
            try:
                # Special handling for _system collection, values are direct JSON objects.
                if collection_name == "_system" and key.startswith("user:"):
                    decoded_map[key] = json.loads(raw_val)
                else:
                    # For all other collections, values are Base64 encoded JSON.
                    decoded_val_bytes = base64.b64decode(raw_val)
                    decoded_map[key] = json.loads(decoded_val_bytes.decode("utf-8"))
            except (json.JSONDecodeError, base64.binascii.Error) as e:
                logging.warning(
                    f"DBClient: Warning - Failed to decode or parse JSON for key '{key}': {e}. Raw: {raw_val}"
                )
                decoded_map[key] = raw_val
        return CollectionItemListResult(message=response.message, items=decoded_map)

    async def collection_query(self, collection_name: str, query: Query) -> Any:
        """Executes a complex query on a specific collection."""
        collection_name_buffer = write_string(collection_name)
        query_json_buffer = write_bytes(query.to_json())
        payload = collection_name_buffer + query_json_buffer
        response = await self._send_command(CMD_COLLECTION_QUERY, payload)
        if response.status != STATUS_OK:
            raise Exception(
                f"COLLECTION_QUERY failed: {get_status_string(response.status)}: {response.message}"
            )

        try:
            result = json.loads(response.data.decode("utf-8"))
            return result
        except json.JSONDecodeError as e:
            logging.error(
                f"DBClient: Error parsing COLLECTION_QUERY result as JSON: {e}. Raw: {response.data.decode('utf-8')}"
            )
            raise Exception(
                f"COLLECTION_QUERY failed: Invalid JSON format for query results."
            )

    def is_session_authenticated(self) -> bool:
        """Returns true if the current client session is authenticated."""
        return self.is_authenticated_session

    def get_authenticated_username(self) -> Optional[str]:
        """Returns the username of the currently authenticated user, or null if not authenticated."""
        return self.authenticated_user

    def close(self):
        """Closes the underlying socket connection."""
        if self.writer:
            self.writer.close()
        self.is_authenticated_session = False
        self.authenticated_user = None


# The client class is ready to be imported and used.
