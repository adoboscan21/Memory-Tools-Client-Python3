# 🐍 Memory Tools Python Client

An asynchronous Python 3 client for the **Memory Tools** database. It uses `asyncio` and `ssl` for efficient and secure communication over TLS.

---

## 🌟 Features

- **Secure by Default:** Establishes encrypted TLS connections to the Memory Tools server.
- **Robust & Resilient:** Features automatic reconnection logic to handle intermittent network issues.
- **Fully Asynchronous:** Built on `asyncio` for high-performance, non-blocking operations.
- **Complete API Coverage:** Supports all protocol operations, including main store, collections, items, indexes, and complex queries.
- **Pythonic Interface:** Can be used as an async context manager (`async with`) for easy and reliable connection handling.

---

## 🚀 Installation

You can install the client from PyPI:

```bash
pip install memory-tools-client
```

To run the tests after cloning the repository:

```bash
python3 -m tests.test
```

---

## 🛠️ Usage

### Basic Example with Context Manager (Recommended)

Using `async with` is the best practice as it automatically handles connecting and closing the client.

```python
import asyncio
from memory_tools_client import MemoryToolsClient

async def main():
    # The client will be automatically connected and closed
    try:
        async with MemoryToolsClient("127.0.0.1", 5876, "admin", "adminpass", reject_unauthorized=False) as client:
            print(f"Connected as '{client.authenticated_user}'")

            # Set a value in the main store
            await client.set("py_key", {"status": "ok"})
            print("✔ Value set in main store.")

            # Get the value back
            result = await client.get("py_key")
            if result.found:
                print(f"✔ Value retrieved: {result.value}")

    except Exception as e:
        print(f"✖ An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Queries

The `Query` class lets you build complex, server-side queries.

```python
import asyncio
import json
from memory_tools_client import MemoryToolsClient, Query

async def run_query_example():
    async with MemoryToolsClient("127.0.0.1", 5876, "admin", "adminpass", reject_unauthorized=False) as client:
        coll_name = "products"
        await client.collection_create(coll_name)

        # Add some data
        products = [
            {"_id": "p1", "category": "electronics", "price": 250},
            {"_id": "p2", "category": "books", "price": 25},
            {"_id": "p3", "category": "electronics", "price": 900},
        ]
        await client.collection_item_set_many(coll_name, products)

        # Query: Find electronics with a price over 500
        query = Query(filter={"and": [
            {"field": "category", "op": "=", "value": "electronics"},
            {"field": "price", "op": ">", "value": 500}
        ]})

        results = await client.collection_query(coll_name, query)
        print("Query Results:")
        print(json.dumps(results, indent=2))

        await client.collection_delete(coll_name)

if __name__ == "__main__":
    asyncio.run(run_query_example())
```

---

## ⚡ API Reference

### Connection and Session

#### `MemoryToolsClient(host, port, username?, password?, server_cert_path?, reject_unauthorized?)`

Creates a new client instance.

- **`host`** (`str`): Server IP address or hostname.
- **`port`** (`int`): Server TLS port.
- **`username`** (`str`, optional): Username for authentication.
- **`password`** (`str`, optional): Password for authentication.
- **`server_cert_path`** (`str`, optional): Path to the server's CA certificate for verification. If `None`, uses system CAs.
- **`reject_unauthorized`** (`bool`, optional): If `False`, disables certificate verification (**not for production**). Defaults to `True`.

#### `async connect()`

Ensures an active connection is established and authenticated. Automatically called by other methods.

#### `async close()`

Gracefully closes the connection. Automatically called when using `async with`.

#### `is_authenticated` (property)

Returns `True` if the client session is currently authenticated.

#### `authenticated_user` (property)

Returns the username (`str`) of the authenticated user, or `None`.

### Main Store Operations

#### `async set(key: str, value: Any, ttl_seconds: int = 0) -> str`

Stores a key-value pair. The `value` is JSON-serialized. A `value` of `None` with `ttl_seconds=0` deletes the key.

#### `async get(key: str) -> GetResult`

Retrieves a key-value pair. Returns a `GetResult` object with `.found` (bool) and `.value` (Any).

### Collection Operations

#### `async collection_create(name: str) -> str`

Creates a new collection.

#### `async collection_delete(name: str) -> str`

Deletes an entire collection and all its items.

#### `async collection_list() -> List[str]`

Returns a list of all collection names.

### Index Operations

#### `async collection_index_create(collection_name: str, field_name: str) -> str`

Creates an index on a field to speed up queries.

#### `async collection_index_delete(collection_name: str, field_name: str) -> str`

Deletes an index from a field.

#### `async collection_index_list(collection_name: str) -> List[str]`

Returns a list of indexed fields for a collection.

### Collection Item Operations

#### `async collection_item_set(collection_name: str, key: str, value: Any, ttl_seconds: int = 0) -> str`

Sets an item within a collection.

#### `async collection_item_set_many(collection_name: str, items: List[Dict]) -> str`

Sets multiple items from a list of dictionaries.

#### `async collection_item_get(collection_name: str, key: str) -> GetResult`

Retrieves a single item from a collection. Returns a `GetResult` object.

#### `async collection_item_delete(collection_name: str, key: str) -> str`

Deletes a single item from a collection by its key.

#### `async collection_item_delete_many(collection_name: str, keys: List[str]) -> str`

Deletes multiple items from a collection by their keys.

#### `async collection_item_list(collection_name: str) -> Dict[str, Any]`

Returns a dictionary of all items in a collection.

### Query Operations

#### `async collection_query(collection_name: str, query: Query) -> Any`

Executes a complex query on a collection.

- **`query`** (`Query`): A `Query` object defining the operation. The `Query` constructor accepts keyword arguments like `filter`, `order_by`, `limit`, `count`, etc.

---

## 🔒 Security Considerations

- **Use TLS:** Always connect over TLS to encrypt data in transit.
- **Verify Certificates:** In production, always set `reject_unauthorized=True` (the default) and provide a `server_cert_path` to your CA certificate. This prevents man-in-the-middle attacks.
- **Manage Credentials:** Avoid hardcoding credentials. Use environment variables or a secrets management system.

---

## 🤝 Contributions

Contributions are welcome! If you find a bug or have an idea, feel free to open an issue or submit a pull request on the GitHub repository.

---

## Support the Project!

Hello! I'm the developer behind **Memory Tools**. This is an open-source project.

I've dedicated a lot of time and effort to this project, and with your support, I can continue to maintain it, add new features, and make it better for everyone.

### How You Can Help

Every contribution is a great help and is enormously appreciated. If you would like to support the continued development of this project, you can make a donation via PayPal.

**[Click here to donate](https://paypal.me/AdonayB?locale.x=es_XC&country.x=VE)**

### Other Ways to Contribute

- **Share the project:** Talk about it on social media or with your friends.
- **Report bugs:** If you find a problem, open an issue on GitHub.
- **Contribute code:** If you have coding skills, you can help improve the code.
  Thank you for your support!
