import asyncio
from src.memory_tools_client import MemoryToolsClient

async def main():
    client_config = {
        "host": "127.0.0.1",
        "port": 5876,
        "username": "admin",
        "password": "adminpass",
        "server_cert_path": None,
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