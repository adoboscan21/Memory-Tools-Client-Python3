# 🐍 Memory Tools Python Client

An asynchronous Python 3 client for the **Memory Tools** database. It uses `asyncio` and `ssl` for efficient and secure communication over TLS.

---

## 🌟 Features

- **Secure by Default:** Establishes encrypted TLS connections to the Memory Tools server.
- **Robust & Resilient:** Features automatic reconnection logic to handle intermittent network issues.
- **Fully Asynchronous:** Built on `asyncio` for high-performance, non-blocking operations.
- **Client-Focused API:** Supports all operations available to a standard remote client, including collections, items, indexes, and complex queries.
- **Pythonic Interface:** Can be used as an async context manager (`async with`) for easy and reliable connection handling.

---

## 🚀 Installation

_Currently, you need to install it directly from the source file._ (Eventually, you could publish it to PyPI).

To run the tests after cloning the repository:

```bash
# First, ensure the server is running in a separate terminal
# go run .

# Then, run the test script
python3 test.py
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
        # Connect as a standard user, e.g., 'admin'
        async with MemoryToolsClient("127.0.0.1", 5876, "admin", "adminpass", None, False) as client:
            print(f"Connected as '{client.authenticated_user}'")

            coll_name = "my_first_collection"
            await client.collection_create(coll_name)
            print(f"✔ Collection '{coll_name}' created.")

            # Set an item in the collection
            item_key = "user:101"
            item_value = {"name": "Alice", "status": "active", "level": 99}
            await client.collection_item_set(coll_name, item_key, item_value)
            print(f"✔ Item '{item_key}' set in collection.")

            # Get the item back
            result = await client.collection_item_get(coll_name, item_key)
            if result.found:
                print(f"✔ Item retrieved: {result.value}")

            # Clean up
            await client.collection_delete(coll_name)
            print(f"✔ Collection '{coll_name}' deleted.")

    except Exception as e:
        print(f"✖ An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🔬 Advanced Queries with the `Query` Object

The `Query` class is the most powerful feature of the client, allowing you to build complex, server-side queries. This is highly efficient as it avoids transferring entire collections over the network.

### Query Parameters

You create a `Query` object by passing keyword arguments:

- `filter` (dict): Defines conditions to select items (like a `WHERE` clause).
- `order_by` (list): Sorts the results based on one or more fields.
- `limit` (int): Restricts the maximum number of results returned.
- `offset` (int): Skips a specified number of results, used for pagination.
- `count` (bool): If `True`, returns a count of matching items instead of the items themselves.
- `distinct` (str): Returns a list of unique values for the specified field.
- `group_by` (list): Groups results by one or more fields to perform aggregations.
- `aggregations` (dict): Defines aggregation functions to run on groups (e.g., `SUM`, `AVG`, `COUNT`).
- `having` (dict): Filters the results _after_ grouping and aggregation (like a `HAVING` clause).
- **`projection` (list)**: Selects which fields to return in the final results, reducing network traffic.
- **`lookups` (list)**: Enriches documents by joining data from other collections in a pipeline, similar to a `LEFT JOIN`.

### Building Filters

The `filter` dictionary is the core of your query. It can be a single condition or a nested structure using logical operators.

**Single Condition Structure:** `{"field": "field_name", "op": "operator", "value": ...}`

| Operator (`op`) | Description                                         | Example `value`            |
| --------------- | --------------------------------------------------- | -------------------------- |
| `=`             | Equal to                                            | `"some_string"` or `123`   |
| `!=`            | Not equal to                                        | `"some_string"` or `123`   |
| `>`             | Greater than                                        | `100`                      |
| `>=`            | Greater than or equal to                            | `100`                      |
| `<`             | Less than                                           | `50`                       |
| `<=`            | Less than or equal to                               | `50`                       |
| `like`          | Case-insensitive pattern matching (`%` is wildcard) | `"start%"` or `"%middle%"` |
| `in`            | Value is in a list of possibilities                 | `["value1", "value2"]`     |
| `between`       | Value is between two values (inclusive)             | `[10, 20]`                 |
| `is null`       | The field does not exist or is `null`               | `True`                     |
| `is not null`   | The field exists and is not `null`                  | `True`                     |

Exportar a Hojas de cálculo

**Logical Operators (`and`, `or`, `not`):**

```python
from memory_tools_client import Query

# Query: Find users who are active AND (live in Madrid OR live in Barcelona)
query = Query(filter={
    "and": [
        {"field": "active", "op": "=", "value": True},
        {"or": [
            {"field": "city", "op": "=", "value": "Madrid"},
            {"field": "city", "op": "=", "value": "Barcelona"}
        ]}
    ]
})
```

### Joins and Data Enrichment with `lookups`

The **`lookups`** parameter enables powerful server-side joins to enrich your query results. It accepts a list of lookup dictionaries, which are executed in a sequence (a pipeline). This is extremely efficient as it avoids making multiple round-trips to the database.

Each lookup dictionary has the following structure:

| Key            | Type  | Description                                                                                             |
| -------------- | ----- | ------------------------------------------------------------------------------------------------------- |
| `from`         | `str` | The name of the collection to join with.                                                                |
| `localField`   | `str` | The field from the current documents. Dot notation is supported for nested fields (e.g., `product.id`). |
| `foreignField` | `str` | The field in the `from` collection to match against.                                                    |
| `as`           | `str` | The name of the new field where the joined data will be stored.                                         |

Exportar a Hojas de cálculo

**Example: Chained Lookups & Projection** Let's query an `inventory_status` collection and enrich it with data from `products` and then `suppliers`.

```python
import json
from memory_tools_client import Query

# Query: Get the status of all items in stock, and for each,
# join the product's name and price, and then the supplier's name.
# Finally, project a clean, flat structure.

report_query = Query(
    # 1. Start with the inventory and filter
    filter={"field": "status", "op": "=", "value": "in_stock"},

    # 2. Perform a pipeline of lookups
    lookups=[
        {
            "from": "products",
            "localField": "productId",
            "foreignField": "_id",
            "as": "product"
        },
        {
            "from": "suppliers",
            "localField": "product.supplierId",
            "foreignField": "_id",
            "as": "supplier"
        }
    ],

    # 3. Select only the fields we need for the final report
    projection=[
        "product.name",
        "stock",
        "supplier.name",
        "supplier.country"
    ]
)

# results = await client.collection_query("inventory_status", report_query)
# print(json.dumps(results, indent=2))
# Expected Output:
# [
#   {
#     "product": { "name": "MechKey Pro" },
#     "stock": 75,
#     "supplier": { "name": "GigaWare", "country": "Taiwan" }
#   },
#   {
#     "product": { "name": "ErgoMouse X" },
#     "stock": 120,
#     "supplier": { "name": "InnoGadget", "country": "USA" }
#   }
# ]
```

### Aggregation Example

You can perform powerful data analysis directly on the server.

```python
# Query: Count the number of active users per city and calculate the average score,
# but only for cities having more than 10 active users.

query = Query(
    # First, select only active users
    filter={"field": "active", "op": "=", "value": True},
    # Group by the 'city' field
    group_by=["city"],
    # Define aggregations to perform on each group
    aggregations={
        "user_count": {"func": "count", "field": "_id"},
        "average_score": {"func": "avg", "field": "score"}
    },
    # Filter the groups after aggregation
    having={"field": "user_count", "op": ">", "value": 10},
    # Order the final results
    order_by=[{"field": "average_score", "direction": "desc"}]
)

# results = await client.collection_query("users", query)
# print(json.dumps(results, indent=2))
```

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

#### `is_authenticated` (property)

Returns `True` if the client session is currently authenticated.

### Collection Operations

#### `async collection_create(name: str) -> CommandResponse`

Ensures a collection with the given name exists.

#### `async collection_delete(name: str) -> CommandResponse`

Deletes an entire collection and all its items.

#### `async collection_list() -> List[str]`

Returns a list of all collection names the user has access to.

### Index Operations

#### `async collection_index_create(collection_name: str, field_name: str) -> CommandResponse`

Creates an index on a field to speed up queries.

#### `async collection_index_delete(collection_name: str, field_name: str) -> CommandResponse`

Deletes an index from a field.

#### `async collection_index_list(collection_name: str) -> List[str]`

Returns a list of indexed fields for a collection.

### Collection Item Operations

#### `async collection_item_set(collection_name: str, key: str, value: Any, ttl_seconds: int = 0) -> CommandResponse`

Sets an item (JSON document) within a collection, identified by its key.

#### `async collection_item_set_many(collection_name: str, items: List[Dict]) -> CommandResponse`

Sets multiple items from a list of dictionaries in a single batch operation. Each dictionary should contain an `_id` key.

#### `async collection_item_update(collection_name: str, key: str, patch_value: Dict) -> CommandResponse`

Partially updates an existing item. Only the fields in `patch_value` will be added or overwritten.

#### `async collection_item_update_many(collection_name: str, items: List[Dict]) -> CommandResponse`

Partially updates multiple items in a single batch. `items` must be a list of dicts with the format: `[{'_id': 'key1', 'patch': {...}}, ...]`.

#### `async collection_item_get(collection_name: str, key: str) -> GetResult`

Retrieves a single item from a collection. Returns a `GetResult` object.

#### `async collection_item_delete(collection_name: str, key: str) -> CommandResponse`

Deletes a single item from a collection by its key.

#### `async collection_item_delete_many(collection_name: str, keys: List[str]) -> CommandResponse`

Deletes multiple items from a collection by their keys in a single batch.

### Query Operations

#### `async collection_query(collection_name: str, query: Query) -> Any`

Executes a complex query on a collection.

- **`query`** (`Query`): A `Query` object defining the operation. See the "Advanced Queries" section for details.

---

## 🔒 Security Considerations

- **Use TLS:** Always connect over TLS to encrypt data in transit.
- **Verify Certificates:** In production, always set `reject_unauthorized=True` (the default) and provide a `server_cert_path` to your CA certificate. This prevents man-in-the-middle attacks.
- **Manage Credentials:** Avoid hardcoding credentials. Use environment variables or a secrets management system.
- **Principle of Least Privilege:** Create users with the minimum permissions they need. Avoid using the `admin` user for regular application access.

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
