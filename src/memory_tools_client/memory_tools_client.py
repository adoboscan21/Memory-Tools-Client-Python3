import asyncio
import ssl
import json
import struct
import base64
from typing import Any, Dict, List, Optional, Union
import logging

# Logging configuration for diagnostics
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Protocol Constants (Synchronized with the Go server) ---
# Listing only the commands relevant for a remote client.
# CMD_SET and CMD_GET (main store) are server-side operations and are omitted.
CMD_COLLECTION_CREATE = 3
CMD_COLLECTION_DELETE = 4
CMD_COLLECTION_LIST = 5
CMD_COLLECTION_INDEX_CREATE = 6
CMD_COLLECTION_INDEX_DELETE = 7
CMD_COLLECTION_INDEX_LIST = 8
CMD_COLLECTION_ITEM_SET = 9
CMD_COLLECTION_ITEM_SET_MANY = 10
CMD_COLLECTION_ITEM_GET = 11
CMD_COLLECTION_ITEM_DELETE = 12
CMD_COLLECTION_QUERY = 14
CMD_COLLECTION_ITEM_DELETE_MANY = 15
CMD_COLLECTION_ITEM_UPDATE = 16
CMD_COLLECTION_ITEM_UPDATE_MANY = 17
CMD_AUTHENTICATE = 18

# --- Server Response Statuses ---
STATUS_OK = 1
STATUS_NOT_FOUND = 2
STATUS_ERROR = 3
STATUS_BAD_COMMAND = 4
STATUS_UNAUTHORIZED = 5
STATUS_BAD_REQUEST = 6

def get_status_string(status: int) -> str:
    """Converts a numeric status code to its string representation."""
    return {
        STATUS_OK: "OK",
        STATUS_NOT_FOUND: "NOT_FOUND",
        STATUS_ERROR: "ERROR",
        STATUS_BAD_COMMAND: "BAD_COMMAND",
        STATUS_UNAUTHORIZED: "UNAUTHORIZED",
        STATUS_BAD_REQUEST: "BAD_REQUEST",
    }.get(status, "UNKNOWN_STATUS")

# --- Data Classes for Responses and Queries ---
class CommandResponse:
    """Base class for all server responses."""
    def __init__(self, status: int, message: str, data: bytes):
        self.status_code = status
        self.status = get_status_string(status)
        self.message = message
        self.raw_data = data

    @property
    def ok(self) -> bool:
        """Returns True if the response status is OK (code 1)."""
        return self.status_code == STATUS_OK

    @property
    def json_data(self) -> Optional[Union[Dict, List]]:
        """
        Attempts to decode the raw response data as JSON.
        Returns None if there is no data or if it's not valid JSON.
        """
        if self.raw_data:
            try:
                return json.loads(self.raw_data)
            except json.JSONDecodeError:
                return None
        return None

    def __repr__(self) -> str:
        return f"<CommandResponse status='{self.status}' message='{self.message}'>"

class GetResult(CommandResponse):
    """Specialized result for 'get' operations."""
    @property
    def found(self) -> bool:
        """Returns True if the item was found (status OK)."""
        return self.ok

    @property
    def value(self) -> Optional[Any]:
        """Returns the item's value decoded as JSON."""
        return self.json_data

class Query:
    """Helper class for building complex queries."""
    def __init__(self, **kwargs):
        self.filter: Optional[Dict] = kwargs.get("filter")
        self.order_by: Optional[List[Dict]] = kwargs.get("order_by")
        self.limit: Optional[int] = kwargs.get("limit")
        self.offset: Optional[int] = kwargs.get("offset")
        self.count: Optional[bool] = kwargs.get("count")
        self.aggregations: Optional[Dict] = kwargs.get("aggregations")
        self.group_by: Optional[List[str]] = kwargs.get("group_by")
        self.having: Optional[Dict] = kwargs.get("having")
        self.distinct: Optional[str] = kwargs.get("distinct")
        self.projection: Optional[List[str]] = kwargs.get("projection")
        self.lookups: Optional[List[Dict]] = kwargs.get("lookups")

    def to_json(self) -> bytes:
        """Serializes the query to a server-compatible JSON."""
        data = {key: value for key, value in self.__dict__.items() if value is not None}
        return json.dumps(data).encode('utf-8')

# --- Binary Protocol Helper Functions ---
def write_string(s: str) -> bytes:
    """Encodes a string with a length prefix (4 bytes, little-endian)."""
    s_bytes = s.encode('utf-8')
    return struct.pack('<L', len(s_bytes)) + s_bytes

def write_bytes(b: bytes) -> bytes:
    """Encodes a byte slice with a length prefix (4 bytes, little-endian)."""
    return struct.pack('<L', len(b)) + b

async def read_n_bytes(reader: asyncio.StreamReader, n: int) -> bytes:
    """Reads exactly N bytes from the stream."""
    data = await reader.readexactly(n)
    if data is None:
        raise ConnectionError("Connection closed while reading.")
    return data

# --- Main Client Class ---
class MemoryToolsClient:
    """Asynchronous client to interact with a Memory Tools server."""
    def __init__(self, host: str, port: int, username: Optional[str] = None, password: Optional[str] = None, server_cert_path: Optional[str] = None, reject_unauthorized: bool = True):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.server_cert_path = server_cert_path
        self.reject_unauthorized = reject_unauthorized
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.authenticated_user: Optional[str] = None
        self._lock = asyncio.Lock()

    @property
    def is_authenticated(self) -> bool:
        """Checks if the client is currently authenticated."""
        return self.authenticated_user is not None

    async def connect(self):
        """
        Establishes the connection with the server if not active.
        Includes reconnection logic and automatic authentication if credentials were provided.
        """
        async with self._lock:
            if self.writer and not self.writer.is_closing():
                return
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()
            ssl_context = ssl.create_default_context(cafile=self.server_cert_path)
            if not self.reject_unauthorized:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)
                logging.info(f"Client: Securely connected to {self.host}:{self.port}")
                if self.username and self.password:
                    await self._perform_authentication(self.username, self.password)
            except Exception as e:
                self.authenticated_user = None
                logging.error(f"Client: Connection failed: {e}")
                raise

    async def _perform_authentication(self, username: str, password: str):
        """Internal logic for the authentication handshake."""
        if not self.writer: raise ConnectionError("Client is not connected.")
        payload = write_string(username) + write_string(password)
        command_buffer = bytes([CMD_AUTHENTICATE]) + payload
        self.writer.write(command_buffer)
        await self.writer.drain()
        status, message, _ = await self._read_response_tuple()
        if status == STATUS_OK:
            self.authenticated_user = username
            logging.info(f"Authentication successful for user '{username}'.")
        else:
            self.authenticated_user = None
            raise PermissionError(f"Authentication failed: {get_status_string(status)}: {message}")

    async def _read_response_tuple(self) -> tuple[int, str, bytes]:
        """Reads a complete server response and returns it as a tuple."""
        if not self.reader: raise ConnectionError("Client is not connected.")
        status = (await read_n_bytes(self.reader, 1))[0]
        msg_len = struct.unpack('<L', await read_n_bytes(self.reader, 4))[0]
        message = (await read_n_bytes(self.reader, msg_len)).decode('utf-8')
        data_len = struct.unpack('<L', await read_n_bytes(self.reader, 4))[0]
        data = await read_n_bytes(self.reader, data_len)
        return status, message, data
    
    async def _read_response(self) -> CommandResponse:
        """Reads a complete response and encapsulates it in a CommandResponse object."""
        status, message, data = await self._read_response_tuple()
        return CommandResponse(status, message, data)

    async def _send_command(self, command_type: int, payload: bytes) -> CommandResponse:
        """Ensures connection, sends a command, and returns the encapsulated response."""
        await self.connect()
        if not self.writer: raise ConnectionError("Client is not connected.")
        if command_type != CMD_AUTHENTICATE and not self.is_authenticated:
            raise PermissionError("Client is not authenticated.")
        self.writer.write(bytes([command_type]) + payload)
        await self.writer.drain()
        return await self._read_response()

    async def close(self):
        """Closes the connection to the server cleanly."""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except ConnectionError:
                pass 
        self.authenticated_user = None
        logging.info("Connection closed.")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --- Public Client API ---
    async def collection_create(self, name: str) -> CommandResponse:
        """Ensures a collection exists."""
        return await self._send_command(CMD_COLLECTION_CREATE, write_string(name))

    async def collection_delete(self, name: str) -> CommandResponse:
        """Deletes a collection and all of its items."""
        return await self._send_command(CMD_COLLECTION_DELETE, write_string(name))

    async def collection_list(self) -> List[str]:
        """Lists the collections the current user has access to."""
        response = await self._send_command(CMD_COLLECTION_LIST, b'')
        if not response.ok: raise Exception(f"Collection List failed: {response.status}: {response.message}")
        return response.json_data or []

    async def collection_index_create(self, collection_name: str, field_name: str) -> CommandResponse:
        """Creates an index on a collection field to speed up queries."""
        payload = write_string(collection_name) + write_string(field_name)
        return await self._send_command(CMD_COLLECTION_INDEX_CREATE, payload)

    async def collection_index_delete(self, collection_name: str, field_name: str) -> CommandResponse:
        """Deletes an index from a collection."""
        payload = write_string(collection_name) + write_string(field_name)
        return await self._send_command(CMD_COLLECTION_INDEX_DELETE, payload)

    async def collection_index_list(self, collection_name: str) -> List[str]:
        """Lists the indexed fields of a collection."""
        response = await self._send_command(CMD_COLLECTION_INDEX_LIST, write_string(collection_name))
        if not response.ok: raise Exception(f"Index List failed: {response.status}: {response.message}")
        return response.json_data or []

    async def collection_item_set(self, collection_name: str, key: str, value: Any, ttl_seconds: int = 0) -> CommandResponse:
        """Saves an item (JSON document) in a collection."""
        payload = (write_string(collection_name) +
                   write_string(key) +
                   write_bytes(json.dumps(value).encode('utf-8')) +
                   struct.pack('<q', ttl_seconds))
        return await self._send_command(CMD_COLLECTION_ITEM_SET, payload)

    async def collection_item_set_many(self, collection_name: str, items: List[Dict]) -> CommandResponse:
        """Saves multiple items in a collection in a single operation."""
        payload = write_string(collection_name) + write_bytes(json.dumps(items).encode('utf-8'))
        return await self._send_command(CMD_COLLECTION_ITEM_SET_MANY, payload)
        
    async def collection_item_update(self, collection_name: str, key: str, patch_value: Dict) -> CommandResponse:
        """Partially updates an item with the fields provided in `patch_value`."""
        payload = (write_string(collection_name) +
                   write_string(key) +
                   write_bytes(json.dumps(patch_value).encode('utf-8')))
        return await self._send_command(CMD_COLLECTION_ITEM_UPDATE, payload)

    async def collection_item_update_many(self, collection_name: str, items: List[Dict]) -> CommandResponse:
        """
        Partially updates multiple items.
        `items` must be a list of dicts with format: `[{'_id': 'key1', 'patch': {...}}, ...]`
        """
        payload = write_string(collection_name) + write_bytes(json.dumps(items).encode('utf-8'))
        return await self._send_command(CMD_COLLECTION_ITEM_UPDATE_MANY, payload)

    async def collection_item_get(self, collection_name: str, key: str) -> GetResult:
        """Gets an item from a collection by its key."""
        payload = write_string(collection_name) + write_string(key)
        response = await self._send_command(CMD_COLLECTION_ITEM_GET, payload)
        return GetResult(response.status_code, response.message, response.raw_data)

    async def collection_item_delete(self, collection_name: str, key: str) -> CommandResponse:
        """Deletes an item from a collection."""
        payload = write_string(collection_name) + write_string(key)
        return await self._send_command(CMD_COLLECTION_ITEM_DELETE, payload)

    async def collection_item_delete_many(self, collection_name: str, keys: List[str]) -> CommandResponse:
        """Deletes multiple items from a collection by their keys."""
        key_payloads = b''.join(write_string(key) for key in keys)
        payload = write_string(collection_name) + struct.pack('<L', len(keys)) + key_payloads
        return await self._send_command(CMD_COLLECTION_ITEM_DELETE_MANY, payload)

    async def collection_query(self, collection_name: str, query: Query) -> Any:
        """Executes a complex query on a collection."""
        payload = write_string(collection_name) + write_bytes(query.to_json())
        response = await self._send_command(CMD_COLLECTION_QUERY, payload)
        if not response.ok: raise Exception(f"Query failed: {response.status}: {response.message}")
        return response.json_data