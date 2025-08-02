import asyncio
import ssl
import json
import struct
import base64
from typing import Any, Dict, List, Optional
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Constantes del Protocolo (deben coincidir con Go) ---
# TIPOS DE COMANDO
CMD_SET = 1
CMD_GET = 2
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
CMD_COLLECTION_ITEM_LIST = 13
CMD_COLLECTION_QUERY = 14
CMD_COLLECTION_ITEM_DELETE_MANY = 15
CMD_AUTHENTICATE = 16

# ESTADOS DE RESPUESTA
STATUS_OK = 1
STATUS_NOT_FOUND = 2
STATUS_ERROR = 3
STATUS_BAD_COMMAND = 4
STATUS_UNAUTHORIZED = 5
STATUS_BAD_REQUEST = 6

def get_status_string(status: int) -> str:
    return {
        STATUS_OK: "OK",
        STATUS_NOT_FOUND: "NOT_FOUND",
        STATUS_ERROR: "ERROR",
        STATUS_BAD_COMMAND: "BAD_COMMAND",
        STATUS_UNAUTHORIZED: "UNAUTHORIZED",
        STATUS_BAD_REQUEST: "BAD_REQUEST",
    }.get(status, "UNKNOWN_STATUS")

# --- Clases de Datos ---
class GetResult:
    def __init__(self, found: bool, message: str, value: Optional[Any]):
        self.found = found
        self.message = message
        self.value = value

class Query:
    def __init__(self, **kwargs):
        self.filter = kwargs.get("filter")
        self.order_by = kwargs.get("order_by")
        self.limit = kwargs.get("limit")
        self.offset = kwargs.get("offset")
        self.count = kwargs.get("count")
        self.aggregations = kwargs.get("aggregations")
        self.group_by = kwargs.get("group_by")
        self.having = kwargs.get("having")
        self.distinct = kwargs.get("distinct")

    def to_json(self) -> bytes:
        data = {key: value for key, value in self.__dict__.items() if value is not None}
        return json.dumps(data).encode('utf-8')

# --- Funciones Auxiliares de Protocolo ---
def write_string(s: str) -> bytes:
    s_bytes = s.encode('utf--8')
    return struct.pack('<L', len(s_bytes)) + s_bytes

def write_bytes(b: bytes) -> bytes:
    return struct.pack('<L', len(b)) + b

async def read_n_bytes(reader: asyncio.StreamReader, n: int) -> bytes:
    return await reader.readexactly(n)

# --- Clase del Cliente ---
class MemoryToolsClient:
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
        return self.authenticated_user is not None

    async def connect(self):
        """
        Establece la conexión si no está activa. Incluye lógica de reconexión automática.
        """
        async with self._lock:
            # Si ya estamos conectados y la conexión es válida, no hacer nada.
            if self.writer and not self.writer.is_closing():
                return

            # Si hay una conexión previa, cerrarla antes de reintentar.
            if self.writer:
                self.writer.close()
                await self.writer.wait_closed()

            ssl_context = ssl.create_default_context(cafile=self.server_cert_path)
            if not self.reject_unauthorized:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            try:
                self.reader, self.writer = await asyncio.open_connection(self.host, self.port, ssl=ssl_context)
                logging.info(f"DBClient: Connected securely to {self.host}:{self.port}")

                if self.username and self.password:
                    await self._perform_authentication(self.username, self.password)
                else:
                    logging.warning("Connected without credentials.")

            except Exception as e:
                self.authenticated_user = None
                logging.error(f"DBClient: Connection failed: {e}")
                raise

    async def _perform_authentication(self, username: str, password: str):
        """Autentica la sesión actual."""
        if not self.writer: raise ConnectionError("Client is not connected.")

        payload = write_string(username) + write_string(password)
        command_buffer = bytes([CMD_AUTHENTICATE]) + payload
        self.writer.write(command_buffer)
        await self.writer.drain()

        status, message, _ = await self._read_response()

        if status == STATUS_OK:
            self.authenticated_user = username
            logging.info(f"Authentication successful for user '{username}'.")
        else:
            self.authenticated_user = None
            raise PermissionError(f"Authentication failed: {get_status_string(status)}: {message}")

    async def _read_response(self) -> tuple[int, str, bytes]:
        """Lee una respuesta completa del servidor."""
        if not self.reader: raise ConnectionError("Client is not connected.")
        
        status = (await read_n_bytes(self.reader, 1))[0]
        msg_len = struct.unpack('<L', await read_n_bytes(self.reader, 4))[0]
        message = (await read_n_bytes(self.reader, msg_len)).decode('utf-8')
        data_len = struct.unpack('<L', await read_n_bytes(self.reader, 4))[0]
        data = await read_n_bytes(self.reader, data_len)
        
        return status, message, data

    async def _send_command(self, command_type: int, payload: bytes) -> tuple[int, str, bytes]:
        """Asegura la conexión, envía un comando y retorna la respuesta."""
        await self.connect()
        if not self.writer: raise ConnectionError("Client is not connected.")
        if command_type != CMD_AUTHENTICATE and not self.is_authenticated:
            raise PermissionError("Client is not authenticated.")

        self.writer.write(bytes([command_type]) + payload)
        await self.writer.drain()
        return await self._read_response()

    async def close(self):
        """Cierra la conexión de forma limpia."""
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except ConnectionError:
                pass # Ignorar errores si la conexión ya estaba rota
        self.authenticated_user = None
        logging.info("Connection closed.")

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # --- Métodos Públicos de la API ---

    async def set(self, key: str, value: Any, ttl_seconds: int = 0) -> str:
        payload = (
            write_string(key) +
            write_bytes(json.dumps(value).encode('utf-8')) +
            struct.pack('<q', ttl_seconds)
        )
        status, message, _ = await self._send_command(CMD_SET, payload)
        if status != STATUS_OK: raise Exception(f"SET failed: {get_status_string(status)}: {message}")
        return message

    async def get(self, key: str) -> GetResult:
        status, message, data = await self._send_command(CMD_GET, write_string(key))
        if status == STATUS_NOT_FOUND: return GetResult(False, message, None)
        if status != STATUS_OK: raise Exception(f"GET failed: {get_status_string(status)}: {message}")
        return GetResult(True, message, json.loads(data))

    async def collection_create(self, name: str) -> str:
        status, message, _ = await self._send_command(CMD_COLLECTION_CREATE, write_string(name))
        if status != STATUS_OK: raise Exception(f"Collection Create failed: {get_status_string(status)}: {message}")
        return message

    async def collection_delete(self, name: str) -> str:
        status, message, _ = await self._send_command(CMD_COLLECTION_DELETE, write_string(name))
        if status != STATUS_OK: raise Exception(f"Collection Delete failed: {get_status_string(status)}: {message}")
        return message

    async def collection_list(self) -> List[str]:
        status, message, data = await self._send_command(CMD_COLLECTION_LIST, b'')
        if status != STATUS_OK: raise Exception(f"Collection List failed: {get_status_string(status)}: {message}")
        return json.loads(data)

    async def collection_index_create(self, collection_name: str, field_name: str) -> str:
        payload = write_string(collection_name) + write_string(field_name)
        status, message, _ = await self._send_command(CMD_COLLECTION_INDEX_CREATE, payload)
        if status != STATUS_OK: raise Exception(f"Index Create failed: {get_status_string(status)}: {message}")
        return message

    async def collection_index_delete(self, collection_name: str, field_name: str) -> str:
        payload = write_string(collection_name) + write_string(field_name)
        status, message, _ = await self._send_command(CMD_COLLECTION_INDEX_DELETE, payload)
        if status != STATUS_OK: raise Exception(f"Index Delete failed: {get_status_string(status)}: {message}")
        return message

    async def collection_index_list(self, collection_name: str) -> List[str]:
        status, message, data = await self._send_command(CMD_COLLECTION_INDEX_LIST, write_string(collection_name))
        if status != STATUS_OK: raise Exception(f"Index List failed: {get_status_string(status)}: {message}")
        return json.loads(data)

    async def collection_item_set(self, collection_name: str, key: str, value: Any, ttl_seconds: int = 0) -> str:
        payload = (
            write_string(collection_name) +
            write_string(key) +
            write_bytes(json.dumps(value).encode('utf-8')) +
            struct.pack('<q', ttl_seconds)
        )
        status, message, _ = await self._send_command(CMD_COLLECTION_ITEM_SET, payload)
        if status != STATUS_OK: raise Exception(f"Item Set failed: {get_status_string(status)}: {message}")
        return message

    async def collection_item_set_many(self, collection_name: str, items: List[Dict]) -> str:
        payload = write_string(collection_name) + write_bytes(json.dumps(items).encode('utf-8'))
        status, message, _ = await self._send_command(CMD_COLLECTION_ITEM_SET_MANY, payload)
        if status != STATUS_OK: raise Exception(f"Item Set Many failed: {get_status_string(status)}: {message}")
        return message

    async def collection_item_get(self, collection_name: str, key: str) -> GetResult:
        payload = write_string(collection_name) + write_string(key)
        status, message, data = await self._send_command(CMD_COLLECTION_ITEM_GET, payload)
        if status == STATUS_NOT_FOUND: return GetResult(False, message, None)
        if status != STATUS_OK: raise Exception(f"Item Get failed: {get_status_string(status)}: {message}")
        return GetResult(True, message, json.loads(data))

    async def collection_item_delete(self, collection_name: str, key: str) -> str:
        payload = write_string(collection_name) + write_string(key)
        status, message, _ = await self._send_command(CMD_COLLECTION_ITEM_DELETE, payload)
        if status != STATUS_OK: raise Exception(f"Item Delete failed: {get_status_string(status)}: {message}")
        return message

    async def collection_item_delete_many(self, collection_name: str, keys: List[str]) -> str:
        key_payloads = b''.join(write_string(key) for key in keys)
        payload = write_string(collection_name) + struct.pack('<L', len(keys)) + key_payloads
        status, message, _ = await self._send_command(CMD_COLLECTION_ITEM_DELETE_MANY, payload)
        if status != STATUS_OK: raise Exception(f"Item Delete Many failed: {get_status_string(status)}: {message}")
        return message

    async def collection_item_list(self, collection_name: str) -> Dict[str, Any]:
        status, message, data = await self._send_command(CMD_COLLECTION_ITEM_LIST, write_string(collection_name))
        if status != STATUS_OK: raise Exception(f"Item List failed: {get_status_string(status)}: {message}")
        
        raw_map = json.loads(data)
        decoded_map = {}
        for key, raw_val in raw_map.items():
            try:
                if collection_name == "_system" and key.startswith("user:"):
                    decoded_map[key] = json.loads(raw_val)
                else:
                    decoded_map[key] = json.loads(base64.b64decode(raw_val))
            except Exception:
                decoded_map[key] = raw_val
        return decoded_map

    async def collection_query(self, collection_name: str, query: Query) -> Any:
        payload = write_string(collection_name) + write_bytes(query.to_json())
        status, message, data = await self._send_command(CMD_COLLECTION_QUERY, payload)
        if status != STATUS_OK: raise Exception(f"Query failed: {get_status_string(status)}: {message}")
        return json.loads(data)