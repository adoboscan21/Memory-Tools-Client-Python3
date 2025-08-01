# 🐍 Memory Tools Python Client

An asynchronous client for the **Memory Tools** database, completely replicated from the Node.js version to Python 3. It uses `asyncio` and `ssl` for efficient and secure communication over TLS.

---

## 🌟 Features

- **Secure Connection (TLS):** Establishes encrypted connections with the Memory Tools server.
- **Full API:** Supports all protocol operations:
  - `set`, `get`: for the main store.
  - `collection_create`, `collection_delete`, `collection_list`: for managing collections.
  - `collection_item_set`, `collection_item_set_many`, `collection_item_get`, `collection_item_delete`, `collection_item_delete_many`: for managing items in collections.
  - `collection_query`: for performing complex queries with filters, sorting, and aggregations.
- **Asynchronous by Design:** Built on `asyncio` for high performance and handling multiple operations without blocking.
- **Easy to Use:** Simple and clear client interface with robust error handling.

---

## 🚀 Installation

You can install the client directly from PyPI:

```bash
pip install memory-tools-client
```

If you download the repository, can run test with

```bash
python3 -m test
```

---

## 🛠️ Usage

### Basic Example

To get started, import the client, establish a connection, and perform a simple operation.

```python
import asyncio
from memory_tools_client import MemoryToolsClient

async def main():
    # Replace with your server details
    host = "127.0.0.1"
    port = 5876
    username = "admin"
    password = "adminpass"

    client = MemoryToolsClient(host, port, username, password, reject_unauthorized=False)

    try:
        await client.connect()
        print(f"Connected as '{client.get_authenticated_username()}'")

        # --- Main store operation example ---
        key = "my_python_key"
        value = {"message": "Hello from Python!", "version": 1}

        await client.set(key, value)
        print(f"✔ Value set for key '{key}'")

        result = await client.get(key)
        if result.found:
            print(f"✔ Value retrieved: {result.value}")

        # --- Collection operation example ---
        collection_name = "users"
        await client.collection_create(collection_name)
        print(f"✔ Collection '{collection_name}' created.")

        user_data = {"_id": "juan_perez", "name": "Juan Pérez", "age": 30}
        await client.collection_item_set(collection_name, user_data["_id"], user_data)
        print(f"✔ Item '{user_data['_id']}' added to the collection.")

    except Exception as e:
        print(f"✖ An error occurred: {e}")
    finally:
        client.close()
        print("Connection closed.")

if __name__ == "__main__":
    asyncio.run(main())
```

### Advanced Queries with `collection_query`

The `Query` class allows you to build complex queries that the Memory Tools server can process efficiently.

```python
import asyncio
from memory_tools_client import MemoryToolsClient, Query, OrderByClause, Aggregation
import json

async def run_query_example():
    client = MemoryToolsClient("127.0.0.1", 5876, "admin", "adminpass", reject_unauthorized=False)
    try:
        await client.connect()
        collection_name = "query_example"
        await client.collection_create(collection_name)

        data = [
            {"_id": "item1", "name": "Alice", "points": 150, "active": True},
            {"_id": "item2", "name": "Bob", "points": 80, "active": False},
            {"_id": "item3", "name": "Charlie", "points": 220, "active": True},
            {"_id": "item4", "name": "David", "points": 120, "active": True},
        ]
        await client.collection_item_set_many(collection_name, data)

        # Query: Get all items with 'points' greater than 100, ordered by 'points' descending.
        query_obj = Query(
            filter={"field": "points", "op": ">", "value": 100},
            orderBy=[OrderByClause(field="points", direction="desc")]
        )

        results = await client.collection_query(collection_name, query_obj)
        print("Query Results (points > 100, ordered by points DESC):")
        print(json.dumps(results, indent=2))

        await client.collection_delete(collection_name)

    except Exception as e:
        print(f"✖ Query error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    asyncio.run(run_query_example())
```

---

## 📄 License

This project is licensed under the MIT License. See the `LICENSE.txt` file for more details.

---

## 🤝 Contributions

Contributions are welcome! If you find a bug or have an idea for a new feature, feel free to open an issue or submit a pull request on the GitHub repository.

---

## Support the Project!

Hello! I'm the developer behind **Memory Tools**. This is an open-source project.

I've dedicated a lot of time and effort to this project, and with your support, I can continue to maintain it, add new features, and make it better for everyone.

---

### How You Can Help

Every contribution, no matter the size, is a great help and is enormously appreciated. If you would like to support the continued development of this project, you can make a donation via PayPal.

You can donate directly to my PayPal account by clicking the link below:

**[Click here to donate](https://paypal.me/AdonayB?locale.x=es_XC&country.x=VE)**

---

### Other Ways to Contribute

If you can't donate, don't worry! You can still help in other ways:

- **Share the project:** Talk about it on social media or with your friends.
- **Report bugs:** If you find a problem, open an issue on GitHub.
- **Contribute code:** If you have coding skills, you can help improve the code.

  Thank you for your support!
