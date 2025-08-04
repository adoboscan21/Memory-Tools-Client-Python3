import asyncio
import logging
from src.memory_tools_client import MemoryToolsClient, Query

# --- Configuration ---
HOST = "127.0.0.1"
PORT = 5876
# We test with the 'admin' user, which is a standard remote user role.
ADMIN_USER = "admin"
ADMIN_PASS = "adminpass"
# Use 'server.crt' if your server requires TLS, otherwise None.
# For local testing, you might not have a valid cert, so reject_unauthorized=False can be useful.
CERT_PATH = None 
REJECT_UNAUTHORIZED = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

async def run_tests():
    """Run comprehensive tests for the MemoryToolsClient."""
    print("🚀 Starting Memory Tools Python client tests...")

    # Test 1: Authentication Failure
    print("\n--- Testing Failed Authentication ---")
    try:
        # This block should fail and raise an exception.
        async with MemoryToolsClient(HOST, PORT, "bad_user", "bad_pass", CERT_PATH, REJECT_UNAUTHORIZED):
            print("✖ TEST FAILED: Authentication with incorrect credentials unexpectedly succeeded.")
    except Exception as e:
        print(f"✔ Success: Authentication failed as expected: {e}")

    # Main Test Suite using a valid admin user
    print("\n--- Starting Main Test Suite (as 'admin' user) ---")
    client = MemoryToolsClient(HOST, PORT, ADMIN_USER, ADMIN_PASS, CERT_PATH, REJECT_UNAUTHORIZED)
    coll_name = "py_test_collection" # Define collection name for the whole suite

    try:
        async with client:
            print(f"✔ Success: Connected and authenticated as '{client.authenticated_user}'")
            
            if not client.is_authenticated:
                raise Exception("`is_authenticated` flag was not set correctly after connection")
            print("✔ Success: `is_authenticated` returned True")
            
            # --- Collection and Index Lifecycle ---
            print("\n--- Testing Collection & Index Lifecycle ---")
            await client.collection_create(coll_name)
            print(f"✔ Success: CREATED collection '{coll_name}'")

            collections = await client.collection_list()
            if coll_name not in collections:
                raise Exception(f"Collection '{coll_name}' not found in list after creation")
            print("✔ Success: LIST verified collection exists")

            await client.collection_index_create(coll_name, "city")
            print("✔ Success: CREATED index on 'city'")

            indexes = await client.collection_index_list(coll_name)
            if "city" not in indexes:
                raise Exception("Index 'city' not found in list after creation")
            print(f"✔ Success: LIST verified index exists: {indexes}")

            # --- Item CRUD, Update and Query Tests ---
            print("\n--- Testing Item Operations (CRUD, Update, Query) ---")
            # This inner try/finally ensures the collection is cleaned up even if a test fails
            try:
                # 1. SET operations
                items_to_set = [
                    {"_id": "item:1", "name": "Alice", "city": "Madrid", "active": True, "points": 100},
                    {"_id": "item:2", "name": "Bob", "city": "Barcelona", "active": True, "points": 200},
                    {"_id": "item:3", "name": "Charlie", "city": "Madrid", "active": False, "points": 150},
                ]
                await client.collection_item_set(coll_name, items_to_set[0]["_id"], items_to_set[0])
                print(f"✔ Success: SET single item '{items_to_set[0]['_id']}'")
                
                await client.collection_item_set_many(coll_name, items_to_set[1:])
                print("✔ Success: SET MANY for remaining items")

                # 2. GET operations and verification
                retrieved = await client.collection_item_get(coll_name, "item:1")
                if not (retrieved.found and retrieved.value == items_to_set[0]):
                    raise Exception(f"GET verification failed for item:1. Got: {retrieved.value}")
                print("✔ Success: GET verified item 'item:1' correctly")

                not_found = await client.collection_item_get(coll_name, "non-existent-key")
                if not_found.found:
                    raise Exception("GET for a non-existent key incorrectly returned 'found=True'")
                print("✔ Success: GET for a non-existent key correctly returned 'found=False'")

                # 3. UPDATE operations and verification
                await client.collection_item_update(coll_name, "item:1", {"active": False, "points": 110})
                print("✔ Success: UPDATE single item 'item:1'")
                
                retrieved_after_update = await client.collection_item_get(coll_name, "item:1")
                if not (retrieved_after_update.found and retrieved_after_update.value["active"] is False and retrieved_after_update.value["points"] == 110):
                    raise Exception("Verification after single UPDATE failed")
                print("✔ Success: Verified single item UPDATE was applied")

                await client.collection_item_update_many(coll_name, [
                    {"_id": "item:2", "patch": {"active": False}},
                    {"_id": "item:3", "patch": {"active": True, "city": "Valencia"}}
                ])
                print("✔ Success: UPDATE MANY items 'item:2' and 'item:3'")

                retrieved_2 = await client.collection_item_get(coll_name, "item:2")
                retrieved_3 = await client.collection_item_get(coll_name, "item:3")
                if not (retrieved_2.value["active"] is False and retrieved_3.value["active"] is True and retrieved_3.value["city"] == "Valencia"):
                    raise Exception("Verification after UPDATE MANY failed")
                print("✔ Success: Verified UPDATE MANY changes were applied")

                # 4. QUERY operations
                query_result = await client.collection_query(coll_name, Query(filter={"field": "city", "op": "=", "value": "Valencia"}))
                if len(query_result) != 1 or query_result[0]["_id"] != "item:3":
                    raise Exception(f"Query result is incorrect. Expected 1 item, got {len(query_result)}")
                print("✔ Success: QUERY returned correct filtered data")

                count_result = await client.collection_query(coll_name, Query(count=True))
                if count_result.get("count") != 3:
                    raise Exception(f"Count query failed. Expected 3, got {count_result.get('count')}")
                print(f"✔ Success: COUNT query returned correct count: {count_result['count']}")
                
                # 5. DELETE operations
                await client.collection_item_delete_many(coll_name, ["item:1", "item:2"])
                print("✔ Success: DELETED MANY items")
                
                final_list = await client.collection_item_list(coll_name)
                if len(final_list) != 1 or "item:3" not in final_list:
                    raise Exception(f"Incorrect items remain after deletion: {list(final_list.keys())}")
                print("✔ Success: LIST verified correct items remain after partial deletion")

            finally:
                # --- Cleanup ---
                print("\n--- Running Final Cleanup ---")
                await client.collection_index_delete(coll_name, "city")
                print("✔ Success: DELETED index 'city'")

                await client.collection_delete(coll_name)
                print(f"✔ Success: DELETED collection '{coll_name}'")
                
                final_collections = await client.collection_list()
                if coll_name in final_collections:
                    raise Exception("Collection still exists after deletion")
                print("✔ Success: Verified collection no longer exists in list")

    except Exception as e:
        print(f"\n--- 💥 TEST SUITE FAILED 💥 ---")
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        if isinstance(e, ConnectionRefusedError):
            print("\nHint: Is the Memory Tools server running on a separate terminal?")
    finally:
        print("\n🏁 All tests completed.")

if __name__ == "__main__":
    # To run this test, first start the Go server: `go run .`
    # Then, run this Python script: `python test.py`
    asyncio.run(run_tests())