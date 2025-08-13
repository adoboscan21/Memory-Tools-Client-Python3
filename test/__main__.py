import asyncio
import uuid
import json
from src.memory_tools_client import MemoryToolsClient, Query, CommandResponse

# --- Test Environment Configuration ---
# Modify these variables if your server runs in another location or with different credentials
HOST = "localhost"
PORT = 5876
USERNAME = "admin"  # A user with write permissions on '*'
PASSWORD = "adminpass"
SERVER_CERT_PATH = None  # Path to the server's certificate
REJECT_UNAUTHORIZED = False

# --- Helper Functions for Printing ---


def print_header(title: str):
    """Prints a header for a test section."""
    print("\n" + "=" * 60)
    print(f"--- {title.upper()} ---")
    print("=" * 60)


def print_step(step: int, description: str):
    """Prints the current step of a test."""
    print(f"\n{step}. {description}...")


def check_response(response: CommandResponse):
    """Checks a response and shows whether it was successful or not."""
    if response.ok:
        print(f"   [SUCCESS]  Message: {response.message}")
        if response.raw_data:
            try:
                # Pretty-print JSON if possible
                parsed_json = json.loads(response.raw_data)
                print(f"   [DATA]     {json.dumps(parsed_json, indent=2)}")
            except json.JSONDecodeError:
                print(f"   [DATA]     {response.raw_data.decode(errors='ignore')}")
    else:
        print(f"   [FAILURE]  Status: {response.status}, Message: {response.message}")


async def main():
    """Main function that orchestrates all tests."""
    client = MemoryToolsClient(
        HOST, PORT, USERNAME, PASSWORD, SERVER_CERT_PATH, REJECT_UNAUTHORIZED
    )

    try:
        await client.connect()
        if not client.is_authenticated:
            print("[FAILURE] Initial client authentication failed. Aborting tests.")
            return

        # --- Collections and Indexes Test ---
        await run_collection_and_index_tests(client)

        # --- CRUD Operations Test ---
        await run_crud_tests(client)

        # --- Bulk Operations Test ---
        await run_bulk_tests(client)

        # --- Transactions Test ---
        await run_transaction_tests(client)

        # --- Complex Queries Test ---
        await run_query_tests(client)

    except Exception as e:
        print(f"\n\nAn unexpected error occurred during tests: {e}")
    finally:
        if client.writer and not client.writer.is_closing():
            await client.close()
            print("\nConnection closed.")


async def run_collection_and_index_tests(client: MemoryToolsClient):
    print_header("Collection and Index Management Tests")
    coll_name = f"test_coll_{uuid.uuid4().hex[:8]}"

    try:
        print_step(1, f"Creating collection '{coll_name}'")
        check_response(await client.collection_create(coll_name))

        print_step(2, "Listing collections to verify creation")
        collections = await client.collection_list()
        if coll_name in collections:
            print(f"   [SUCCESS]  Collection '{coll_name}' was found in the list.")
        else:
            print(f"   [FAILURE]  Collection '{coll_name}' was NOT found.")

        print_step(3, "Creating an index on the 'city' field")
        check_response(await client.collection_index_create(coll_name, "city"))

        print_step(4, "Listing indexes to verify creation")
        indexes = await client.collection_index_list(coll_name)
        if "city" in indexes:
            print("   [SUCCESS]  The 'city' index was found.")
        else:
            print("   [FAILURE]  The 'city' index was NOT found.")

        print_step(5, "Deleting the 'city' index")
        check_response(await client.collection_index_delete(coll_name, "city"))

        print_step(6, "Listing indexes to verify deletion")
        indexes_after_delete = await client.collection_index_list(coll_name)
        if "city" not in indexes_after_delete:
            print("   [SUCCESS]  The 'city' index no longer exists.")
        else:
            print("   [FAILURE]  The 'city' index still exists.")

    finally:
        print_step(7, f"Cleanup: deleting collection '{coll_name}'")
        check_response(await client.collection_delete(coll_name))


async def run_crud_tests(client: MemoryToolsClient):
    print_header("CRUD Operations Tests (Create, Read, Update, Delete)")
    coll_name = f"crud_coll_{uuid.uuid4().hex[:8]}"
    item_key = "user-001"

    try:
        await client.collection_create(coll_name)

        print_step(1, "SET (Create) an item")
        check_response(
            await client.collection_item_set(
                coll_name, {"name": "Luisa", "age": 40}, key=item_key
            )
        )

        print_step(2, "GET (Read) the item")
        get_resp = await client.collection_item_get(coll_name, item_key)
        check_response(get_resp)
        if get_resp.found:
            assert get_resp.value["name"] == "Luisa"

        print_step(3, "UPDATE the item")
        check_response(
            await client.collection_item_update(
                coll_name, item_key, {"age": 41, "status": "active"}
            )
        )

        print_step(4, "GET (Read) to verify the update")
        get_updated_resp = await client.collection_item_get(coll_name, item_key)
        check_response(get_updated_resp)
        if get_updated_resp.found:
            assert get_updated_resp.value["age"] == 41

        print_step(5, "DELETE the item")
        check_response(await client.collection_item_delete(coll_name, item_key))

        print_step(6, "GET (Read) to verify the deletion")
        get_deleted_resp = await client.collection_item_get(coll_name, item_key)
        if not get_deleted_resp.found:
            print(f"   [SUCCESS]  Item '{item_key}' was not found, as expected.")
        else:
            print(f"   [FAILURE]  Item '{item_key}' still exists.")

    finally:
        await client.collection_delete(coll_name)


async def run_bulk_tests(client: MemoryToolsClient):
    print_header("Bulk Operations Tests (set_many, delete_many)")
    coll_name = f"bulk_coll_{uuid.uuid4().hex[:8]}"

    try:
        await client.collection_create(coll_name)

        items_to_set = [{"_id": f"item-{i}", "val": i * 10} for i in range(5)]
        keys_to_delete = ["item-1", "item-3"]

        print_step(1, "SET MANY: Inserting 5 items")
        check_response(await client.collection_item_set_many(coll_name, items_to_set))

        print_step(2, "DELETE MANY: Deleting 2 out of 5 items")
        check_response(
            await client.collection_item_delete_many(coll_name, keys_to_delete)
        )

        # --- SOLUTION: Add a small pause here ---
        # This gives the server time to stabilize its in-memory state before the query.
        await asyncio.sleep(0.1)

        print_step(3, "Verifying the final state")
        final_items = await client.collection_query(coll_name, Query())
        final_keys = {item["_id"] for item in final_items}

        # The correct verification: item-0, item-2, and item-4 should remain
        expected_keys = {"item-0", "item-2", "item-4"}

        if final_keys == expected_keys:
            print("   [SUCCESS]  The collection's state is as expected.")
        else:
            print(f"   [FAILURE]  The final state is not correct.")
            print(f"            - Expected: {expected_keys}")
            print(f"            - Found: {final_keys}")

    finally:
        await client.collection_delete(coll_name)


async def run_transaction_tests(client: MemoryToolsClient):
    print_header("Transaction Tests (Commit and Rollback)")
    coll_name = f"tx_coll_{uuid.uuid4().hex[:8]}"
    key_commit = "committed-key"
    key_rollback = "rolled-back-key"

    try:
        await client.collection_create(coll_name)

        # --- COMMIT Test ---
        print_step(1, "Testing a successful COMMIT")
        await client.begin()
        await client.collection_item_set(
            coll_name, {"tx_status": "final"}, key=key_commit
        )
        await client.commit()
        get_committed = await client.collection_item_get(coll_name, key_commit)
        if get_committed.found:
            print(
                "   [SUCCESS]  The item written in the transaction was found after commit."
            )
        else:
            print("   [FAILURE]  The item was not found after commit.")

        # --- ROLLBACK Test ---
        print_step(2, "Testing a ROLLBACK")
        await client.begin()
        await client.collection_item_set(
            coll_name, {"tx_status": "temporary"}, key=key_rollback
        )
        await client.rollback()
        get_rolled_back = await client.collection_item_get(coll_name, key_rollback)
        if not get_rolled_back.found:
            print(
                "   [SUCCESS]  The item written in the transaction was not found after rollback, as expected."
            )
        else:
            print("   [FAILURE]  An item that should have been rolled back was found.")

    finally:
        await client.collection_delete(coll_name)


async def run_query_tests(client: MemoryToolsClient):
    print_header("Query Tests (Filter, Projection, Lookup)")
    users_coll = f"users_{uuid.uuid4().hex[:8]}"
    profiles_coll = f"profiles_{uuid.uuid4().hex[:8]}"

    try:
        await client.collection_create(users_coll)
        await client.collection_create(profiles_coll)

        users = [
            {"_id": "u1", "name": "Elena", "age": 34, "active": True},
            {"_id": "u2", "name": "Marcos", "age": 25, "active": True},
            {"_id": "u3", "name": "Sofia", "age": 45, "active": False},
        ]
        profiles = [
            {"_id": "p1", "user_id": "u1", "city": "Madrid"},
            {"_id": "p2", "user_id": "u2", "city": "Bogota"},
        ]
        await client.collection_item_set_many(users_coll, users)
        await client.collection_item_set_many(profiles_coll, profiles)
        await asyncio.sleep(0.1)  # Give the server time to process writes

        print_step(1, "Query with Filter: active users with age > 30")
        q_filter = Query(
            filter={
                "and": [
                    {"field": "active", "op": "=", "value": True},
                    {"field": "age", "op": ">", "value": 30},
                ]
            }
        )
        result = await client.collection_query(users_coll, q_filter)
        check_response(
            CommandResponse(1, "Query executed", json.dumps(result).encode())
        )
        assert len(result) == 1 and result[0]["name"] == "Elena"

        print_step(2, "Query with Projection: get only name and age")
        q_proj = Query(projection=["name", "age"])
        result = await client.collection_query(users_coll, q_proj)
        check_response(
            CommandResponse(1, "Query executed", json.dumps(result).encode())
        )
        assert all("active" not in user for user in result)

        print_step(3, "Query with Lookup (JOIN): join profiles with users")
        q_lookup = Query(
            lookups=[
                {
                    "from": users_coll,
                    "localField": "user_id",
                    "foreignField": "_id",
                    "as": "user_info",
                }
            ]
        )
        result = await client.collection_query(profiles_coll, q_lookup)
        check_response(
            CommandResponse(1, "Query executed", json.dumps(result).encode())
        )
        profile1 = next(p for p in result if p["_id"] == "p1")
        assert profile1["user_info"]["name"] == "Elena"

    finally:
        await client.collection_delete(users_coll)
        await client.collection_delete(profiles_coll)


if __name__ == "__main__":
    print("Starting test suite for MemoryToolsClient...")
    asyncio.run(main())
    print("\nTest suite finished.")
