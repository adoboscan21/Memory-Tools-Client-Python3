# 🐍 Memory Tools Python Client

An asynchronous Python 3 client for **Memory Tools**, your high-performance, sharded in-memory key-value and document store. It uses `asyncio` and `ssl` for efficient and secure communication over TLS.

- **Project Repository**: [https://github.com/adoboscan21/Memory-Tools](https://github.com/adoboscan21/Memory-Tools)

---

## 🌟 Features

- **🔒 Secure by Default:** Establishes encrypted TLS connections to the Memory Tools server.
- **🔄 Robust & Resilient:** Features automatic reconnection logic to handle intermittent network issues.
- **⚡ Fully Asynchronous:** Built on `asyncio` for high-performance, non-blocking operations.
- **📚 Rich API:** Supports transactions, collections, items, indexes, and complex queries.
- **🐍 Pythonic Interface:** Can be used as an async context manager (`async with`) for easy and reliable connection handling.

---

## 🛠️ Usage

### Connection

There are two ways to manage the connection:

#### 1. Context Manager (Recommended)

Using `async with` is the best practice, as it automatically handles connecting and closing the client, even if errors occur.

```python
import asyncio
from memory_tools_client import MemoryToolsClient

async def main():
    client_config = {
        "host": "127.0.0.1",
        "port": 5876,
        "username": "admin",
        "password": "adminpass",
        "cert_path": None,
        "reject_unauthorized": False
    }

    try:
        async with MemoryToolsClient(**client_config) as client:
            print(f"Connected as '{client.authenticated_user}'")
            # ... perform operations here ...
            response = await client.collection_create("my_collection")
            print(f"Server response: {response.message}")

    except Exception as e:
        print(f"✖️ An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

#### 2. Manual Connection Management

For long-running applications where the client needs to persist, you can manage the connection manually.

```python
client = MemoryToolsClient(**client_config)
try:
    await client.connect()
    if client.is_authenticated:
        # ... use the client ...
        pass
finally:
    await client.close()
```

---

## ✨ ACID Transactions

Memory Tools supports **ACID transactions** to guarantee data integrity across multiple operations. A transaction is an atomic "all-or-nothing" unit of work.

- **`await client.begin()`**: Starts a new transaction.
- **`await client.commit()`**: Atomically applies all queued operations. If any fail, the server rolls back the entire transaction.
- **`await client.rollback()`**: Manually discards all queued operations and ends the transaction.

### Transaction Example: Bank Transfer

```python
import asyncio
from memory_tools_client import MemoryToolsClient

async def run_transfer(client: MemoryToolsClient):
    accounts_coll = "accounts"
    await client.collection_create(accounts_coll)
    await client.collection_item_set_many(accounts_coll, [
        {"_id": "acc_a", "balance": 100},
        {"_id": "acc_b", "balance": 50}
    ])
    print(f"Initial balances: A=${(await client.collection_item_get(accounts_coll, 'acc_a')).value['balance']}, B=${(await client.collection_item_get(accounts_coll, 'acc_b')).value['balance']}")

    try:
        await client.begin()
        print("✔ Transaction started.")

        # 1. Debit from Account A
        await client.collection_item_update(accounts_coll, "acc_a", {"balance": 70})
        # 2. Credit Account B
        await client.collection_item_update(accounts_coll, "acc_b", {"balance": 80})

        await client.commit()
        print("✔ Transaction committed.")

    except Exception as e:
        print(f"✖️ Error: {e}. Rolling back transaction...")
        await client.rollback()
        print("✔ Transaction rolled back.")

    finally:
        # Verify final state
        acc_a = await client.collection_item_get(accounts_coll, "acc_a")
        acc_b = await client.collection_item_get(accounts_coll, "acc_b")
        print(f"Final balances: A=${acc_a.value['balance']}, B=${acc_b.value['balance']}")
        await client.collection_delete(accounts_coll)

# To run:
# async with MemoryToolsClient(...) as client:
#     await run_transfer(client)
```

---

## 🔬 Advanced Queries with `Query`

The `Query` class allows you to build complex queries that run on the server, minimizing network data transfer.

### `Query` Parameters

- `filter` (dict): Conditions to select items (like a `WHERE` clause).
- `order_by` (list): Sorts the results.
- `limit` (int): Maximum number of results.
- `offset` (int): Skips a number of results (for pagination).
- `count` (bool): Returns only the count of matching items.
- `distinct` (str): Returns unique values for a field.
- `group_by` (list): Groups results for aggregations.
- `aggregations` (dict): Functions to run on groups (`SUM`, `AVG`, etc.).
- `having` (dict): Filters results _after_ aggregation.
- `projection` (list): Selects which fields to return.
- `lookups` (list): Enriches documents by joining data from other collections (similar to a `LEFT JOIN`).

### Building Filters

A single condition's structure is: `{"field": "field_name", "op": "operator", "value": ...}`.

| Operator (`op`) | Description                        | Example `value`            |
| --------------- | ---------------------------------- | -------------------------- |
| `=`             | Equal to                           | `"text"` or `123`          |
| `!=`            | Not equal to                       | `"text"` or `123`          |
| `>`             | Greater than                       | `100`                      |
| `>=`            | Greater than or equal to           | `100`                      |
| `<`             | Less than                          | `50`                       |
| `<=`            | Less than or equal to              | `50`                       |
| `like`          | Pattern matching (`%` is wildcard) | `"start%"` or `"%middle%"` |
| `in`            | Value is in a list                 | `["value1", "value2"]`     |
| `between`       | Value is between two values        | `[10, 20]`                 |
| `is null`       | Field does not exist or is `null`  | `True`                     |
| `is not null`   | Field exists and is not `null`     | `True`                     |

You can combine conditions with the logical operators `and`, `or`, and `not`.

```python
from memory_tools_client import Query

# Query: Find users who are active AND (live in Madrid OR live in Bogotá)
query = Query(filter={
    "and": [
        {"field": "active", "op": "=", "value": True},
        {"or": [
            {"field": "city", "op": "=", "value": "Madrid"},
            {"field": "city", "op": "=", "value": "Bogotá"}
        ]}
    ]
})
```

### Joins (`lookups`) and Projection

**`lookups`** perform server-side joins. They are extremely efficient for enriching documents.

**Example:** Get inventory, join it with product information, and then with supplier data.

```python
import json
from memory_tools_client import Query

report_query = Query(
    # 1. Filter the initial inventory
    filter={"field": "status", "op": "=", "value": "in_stock"},

    # 2. Perform a pipeline of joins
    lookups=[
        {
            "from": "products",       # Join with the 'products' collection
            "localField": "productId",  # Field from 'inventory_status'
            "foreignField": "_id",      # Field in 'products'
            "as": "product_info"        # Store result in 'product_info'
        },
        {
            "from": "suppliers",
            "localField": "product_info.supplierId", # Use a field from the already-joined document
            "foreignField": "_id",
            "as": "supplier_info"
        }
    ],

    # 3. Project only the fields needed for the final report
    projection=[
        "product_info.name",
        "stock",
        "supplier_info.name",
        "supplier_info.country"
    ]
)

# results = await client.collection_query("inventory_status", report_query)
# print(json.dumps(results, indent=2))
```

---

## ⚡ API Reference

### Connection and Session

- **`MemoryToolsClient(host, port, username?, password?, server_cert_path?, reject_unauthorized?)`**: Creates a client instance.
- **`async connect()`**: Manually connects and authenticates.
- **`async close()`**: Closes the connection.
- **`is_authenticated`** (property): Returns `True` if the client is authenticated.

### Transaction Operations

- **`async begin() -> CommandResponse`**: Starts a transaction.
- **`async commit() -> CommandResponse`**: Commits the current transaction.
- **`async rollback() -> CommandResponse`**: Rolls back the current transaction.

### Collection Operations

- **`async collection_create(name: str) -> CommandResponse`**: Creates a collection.
- **`async collection_delete(name: str) -> CommandResponse`**: Deletes a collection.
- **`async collection_list() -> List[str]`**: Lists collection names.

### Index Operations

- **`async collection_index_create(collection_name: str, field_name: str) -> CommandResponse`**: Creates an index on a field.
- **`async collection_index_delete(collection_name: str, field_name: str) -> CommandResponse`**: Deletes an index.
- **`async collection_index_list(collection_name: str) -> List[str]`**: Lists a collection's indexed fields.

### Item Operations (CRUD)

- **`async collection_item_set(collection_name, value, key?, ttl_seconds?) -> CommandResponse`**: Creates or replaces an item. A UUID is generated if `key` is not provided.
- **`async collection_item_set_many(collection_name, items: List[Dict]) -> CommandResponse`**: Inserts multiple items. Each `dict` in the list must have an `_id` key.
- **`async collection_item_get(collection_name, key) -> GetResult`**: Retrieves an item. The result has `.found` (bool) and `.value` (dict) properties.
- **`async collection_item_update(collection_name, key, patch_value) -> CommandResponse`**: Partially updates an item.
- **`async collection_item_update_many(collection_name, items: List[Dict]) -> CommandResponse`**: Partially updates multiple items. Format is `[{'_id': 'k1', 'patch': {...}}, ...]`.
- **`async collection_item_delete(collection_name, key) -> CommandResponse`**: Deletes an item.
- **`async collection_item_delete_many(collection_name, keys: List[str]) -> CommandResponse`**: Deletes multiple items by their keys.

### Query Operations

- **`async collection_query(collection_name, query: Query) -> List[Dict]`**: Executes an advanced query and returns a list of documents.

---

## 🔒 Security Considerations

- **Use TLS:** Always connect over TLS to encrypt data in transit.
- **Verify Certificates:** In production, ensure `reject_unauthorized=True` (the default) and provide the path to your CA certificate with `server_cert_path`.
- **Manage Credentials:** Avoid hardcoding credentials. Use environment variables or a secrets management system.
- **Principle of Least Privilege:** Create users with the minimum permissions they need. Avoid using `admin` or `root` for application access.

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
