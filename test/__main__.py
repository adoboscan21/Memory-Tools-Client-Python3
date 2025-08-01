import asyncio
import logging
from src.memory_tools_client import (
    MemoryToolsClient,
    Query,
    OrderByClause,
    Aggregation,
)

# Disable client info logs to keep the test output clean
logging.getLogger("memory_tools_client").setLevel(logging.WARNING)


async def run_tests():
    """
    Runs a series of integration tests for the Memory Tools client.
    Requires a running Memory Tools server at 127.0.0.1:8080.
    """
    print("Starting Memory Tools client tests...")

    # --- Test: Failed Authentication (incorrect user/password) ---
    print("\n--- Test: Failed Authentication (incorrect user/password) ---")
    client_bad_auth = None
    try:
        start_time = asyncio.get_event_loop().time()
        client_bad_auth = MemoryToolsClient(
            "127.0.0.1", 8080, "nonexistent_user", "wrongpassword", None, False
        )
        await client_bad_auth.connect()
        print(
            "✖ Error: Authentication with incorrect credentials unexpectedly succeeded."
        )
    except Exception as error:
        end_time = asyncio.get_event_loop().time()
        duration = end_time - start_time
        print(f"✔ Success: Authentication failed as expected: {error}")
        print(f"Auth_Failed: {duration:.3f}s")
    finally:
        if client_bad_auth:
            client_bad_auth.close()

    # --- Test: Operations with 'root' User (localhost only) ---
    print("\n--- Test: Operations with 'root' User (localhost only) ---")
    client_root = None
    try:
        client_root = MemoryToolsClient(
            "127.0.0.1", 8080, "root", "rootpass", None, False
        )
        start_time = asyncio.get_event_loop().time()
        await client_root.connect()
        end_time = asyncio.get_event_loop().time()
        print(f"Root_ConnectAuth: {end_time - start_time:.3f}s")
        print(
            f"✔ Success: Connected and authenticated as: {client_root.get_authenticated_username()}"
        )

        # Test creating the _system collection (should already exist, but verifies permissions)
        start_time = asyncio.get_event_loop().time()
        create_system_response = await client_root.collection_create("_system")
        end_time = asyncio.get_event_loop().time()
        print(f"Create_System_Collection: {end_time - start_time:.3f}s")
        print(
            f"✔ Success: Tried to create _system collection: {create_system_response}"
        )
    except Exception as error:
        print(f"✖ Error: Unexpected error during root tests: {error}")
    finally:
        if client_root:
            client_root.close()

    # --- Test: Operations with 'admin' User (remote access) ---
    print("\n--- Test: Operations with 'admin' User ---")
    client_admin = None
    try:
        client_admin = MemoryToolsClient(
            "127.0.0.1", 8080, "admin", "adminpass", None, False
        )

        print("Attempting to connect and authenticate as 'admin'...")
        start_time = asyncio.get_event_loop().time()
        await client_admin.connect()
        end_time = asyncio.get_event_loop().time()
        print(f"Admin_ConnectAuth: {end_time - start_time:.3f}s")
        print(
            f"✔ Success: Connected and authenticated as: {client_admin.get_authenticated_username()}"
        )

        # --- Performing Main Store Operations ---
        print("\n--- Performing Main Store Operations as 'admin' ---")

        test_key = "my_python_key"
        test_value = {
            "data": "Python main store test",
            "timestamp": "some_timestamp",
        }
        updated_value = {"data": "Python main store updated", "status": "ok"}

        # 1. Set a key-value pair
        start_time = asyncio.get_event_loop().time()
        await client_admin.set(test_key, test_value, 60)
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Set_1: {end_time - start_time:.3f}s")
        print(f"✔ Success: Key '{test_key}' set in main store.")

        # 2. Get the key-value pair
        start_time = asyncio.get_event_loop().time()
        retrieved_main_item = await client_admin.get(test_key)
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Get_1: {end_time - start_time:.3f}s")
        if retrieved_main_item.found and retrieved_main_item.value == test_value:
            print(
                f"✔ Success: Key '{test_key}' retrieved from main store: {retrieved_main_item.value}"
            )
        else:
            print(
                f"✖ Error: Failed to retrieve or verify key '{test_key}' from main store."
            )

        # 3. Update the key-value pair
        start_time = asyncio.get_event_loop().time()
        await client_admin.set(test_key, updated_value)
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Set_2_Update: {end_time - start_time:.3f}s")
        print(f"✔ Success: Key '{test_key}' updated in main store.")

        start_time = asyncio.get_event_loop().time()
        updated_retrieved_main_item = await client_admin.get(test_key)
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Get_2_Updated: {end_time - start_time:.3f}s")
        if (
            updated_retrieved_main_item.found
            and updated_retrieved_main_item.value == updated_value
        ):
            print(
                f"✔ Success: Key '{test_key}' retrieved after update: {updated_retrieved_main_item.value}"
            )
        else:
            print(
                f"✖ Error: Failed to retrieve or verify updated key '{test_key}' from main store."
            )

        # 4. Try to get a non-existent key
        start_time = asyncio.get_event_loop().time()
        non_existent_item = await client_admin.get("non_existent_key")
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Get_NonExistent: {end_time - start_time:.3f}s")
        if not non_existent_item.found:
            print(
                "✔ Success: Non-existent key 'non_existent_key' not found as expected."
            )
        else:
            print("✖ Error: Non-existent key 'non_existent_key' found unexpectedly.")

        # 5. Delete the key-value pair
        start_time = asyncio.get_event_loop().time()
        await client_admin.set(
            test_key, None, 0
        )  # Setting to None with 0 TTL effectively deletes it
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Delete: {end_time - start_time:.3f}s")

        start_time = asyncio.get_event_loop().time()
        deleted_item = await client_admin.get(test_key)
        end_time = asyncio.get_event_loop().time()
        print(f"MainStore_Get_AfterDelete: {end_time - start_time:.3f}s")
        if not deleted_item.found:
            print(f"✔ Success: Key '{test_key}' deleted from main store.")
        else:
            print(f"✖ Error: Failed to delete key '{test_key}' from main store.")

        # --- Performing Collection Operations ---
        print("\n--- Performing Collection Operations as 'admin' ---")

        test_collection_name = "my_python_data"
        item_key1 = "item_from_python_1"
        item_value1 = {"message": "Hello from Python item 1!"}
        item_key2 = "item_from_python_2"
        item_value2 = {"message": "Hello from Python item 2!", "version": 2}
        item_value2_updated = {"message": "Updated item 2!", "version": 2.1}

        # 1. List collections (before creation)
        start_time = asyncio.get_event_loop().time()
        collections_before_create = await client_admin.collection_list()
        end_time = asyncio.get_event_loop().time()
        print(f"Collection_List_BeforeCreate: {end_time - start_time:.3f}s")
        print("Available collections (before create):", collections_before_create.names)

        # 2. Create a collection
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_create(test_collection_name)
        end_time = asyncio.get_event_loop().time()
        print(f"Collection_Create: {end_time - start_time:.3f}s")
        print(f"✔ Success: Collection '{test_collection_name}' created.")

        # 3. List collections (after creation)
        start_time = asyncio.get_event_loop().time()
        collections_after_create = await client_admin.collection_list()
        end_time = asyncio.get_event_loop().time()
        print(f"Collection_List_AfterCreate: {end_time - start_time:.3f}s")
        if test_collection_name in collections_after_create.names:
            print(
                f"✔ Success: Collection '{test_collection_name}' found in list after creation."
            )
        else:
            print(
                f"✖ Error: Collection '{test_collection_name}' not found in list after creation."
            )

        # 4. Set an item in the collection
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_set(
            test_collection_name, item_key1, item_value1
        )
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_Set_1: {end_time - start_time:.3f}s")
        print(f"✔ Success: Item '{item_key1}' set in '{test_collection_name}'.")

        # 5. Set another item in the collection with TTL
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_set(
            test_collection_name, item_key2, item_value2, 10
        )  # TTL of 10 seconds
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_Set_2_TTL: {end_time - start_time:.3f}s")
        print(
            f"✔ Success: Item '{item_key2}' set in '{test_collection_name}' with TTL."
        )

        # --- NEW TEST: collection item set many ---
        print("\n--- Test: collection item set many ---")
        many_items = [
            {"_id": "many_item_a", "name": "Python Item A", "value": 100},
            {"_id": "many_item_b", "name": "Python Item B", "value": 200},
            {"_id": "many_item_c", "name": "Python Item C", "value": 300},
        ]
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_set_many(test_collection_name, many_items)
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_SetMany: {end_time - start_time:.3f}s")
        print(f"✔ Success: Set many items into collection '{test_collection_name}'.")

        start_time = asyncio.get_event_loop().time()
        all_items_after_set_many = await client_admin.collection_item_list(
            test_collection_name
        )
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_List_AfterSetMany: {end_time - start_time:.3f}s")
        found_keys = list(all_items_after_set_many.items.keys())
        expected_keys = [
            item_key1,
            item_key2,
            "many_item_a",
            "many_item_b",
            "many_item_c",
        ]
        if len(found_keys) == len(expected_keys) and sorted(found_keys) == sorted(
            expected_keys
        ):
            print("✔ Success: All expected keys found after set many.")
            print("Items in collection:", all_items_after_set_many.items)
        else:
            print("✖ Error: Keys are missing or unexpected after set many.")
            print("Expected:", expected_keys)
            print("Found:", found_keys)

        # --- NEW TEST: collection item delete many ---
        print("\n--- Test: collection item delete many ---")
        keys_to_delete = ["many_item_a", "many_item_b"]
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_delete_many(
            test_collection_name, keys_to_delete
        )
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_DeleteMany: {end_time - start_time:.3f}s")
        print(f"✔ Success: Deleted multiple items from '{test_collection_name}'.")

        # Verify that the items are actually deleted
        start_time = asyncio.get_event_loop().time()
        items_after_delete_many = await client_admin.collection_item_list(
            test_collection_name
        )
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_List_AfterDeleteMany: {end_time - start_time:.3f}s")
        keys_after_delete_many = list(items_after_delete_many.items.keys())

        # We expect 3 items remaining: item_key1, item_key2, and many_item_c
        expected_keys_after_delete_many = [item_key1, item_key2, "many_item_c"]
        if len(keys_after_delete_many) == 3 and sorted(
            keys_after_delete_many
        ) == sorted(expected_keys_after_delete_many):
            print(
                "✔ Success: Items were correctly deleted. Remaining keys:",
                keys_after_delete_many,
            )
        else:
            print(
                "✖ Error: Deletion of multiple items failed. Remaining keys:",
                keys_after_delete_many,
            )
        # --- END NEW TEST ---

        # --- Test COLLECTION_QUERY ---
        print("\n--- Test: COLLECTION_QUERY ---")
        query_collection_name = "users_test_py"

        # Data preparation for query test
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_create(query_collection_name)
        user_data = [
            {
                "_id": "juan_perez",
                "name": "Juan Pérez",
                "email": "juan.perez@example.com",
                "age": 32,
                "city": "Madrid",
                "role": "user",
                "status": "active",
                "created_at": "2025-07-28T10:00:00Z",
            },
            {
                "_id": "maria_garcia",
                "name": "María García",
                "email": "maria.garcia@example.com",
                "age": 28,
                "city": "Barcelona",
                "role": "admin",
                "status": "active",
                "created_at": "2025-07-28T11:00:00Z",
            },
            {
                "_id": "luis_sanchez",
                "name": "Luis Sánchez",
                "email": "luis.sanchez@test.com",
                "age": 45,
                "city": "Valencia",
                "role": "user",
                "status": "inactive",
                "created_at": "2025-07-28T12:00:00Z",
            },
            {
                "_id": "ana_diaz",
                "name": "Ana Díaz",
                "email": "ana.diaz@test.com",
                "age": 25,
                "city": "Sevilla",
                "role": "user",
                "status": "active",
                "created_at": "2025-07-28T13:00:00Z",
            },
            {
                "_id": "pedro_lopez",
                "name": "Pedro López",
                "email": "pedro.lopez@outlook.com",
                "age": 38,
                "city": "Bilbao",
                "role": "moderator",
                "status": "active",
                "created_at": "2025-07-28T14:00:00Z",
            },
            {
                "_id": "sofia_ruiz",
                "name": "Sofía Ruiz",
                "email": "sofia.ruiz@example.com",
                "age": 35,
                "city": "Madrid",
                "role": "admin",
                "status": "active",
                "created_at": "2025-07-28T15:00:00Z",
            },
            {
                "_id": "david_blanco",
                "name": "David Blanco",
                "email": "david.blanco@example.com",
                "age": 40,
                "city": "Barcelona",
                "role": "user",
                "status": "active",
                "created_at": "2025-07-28T16:00:00Z",
            },
            {
                "_id": "elena_castro",
                "name": "Elena Castro",
                "email": "elena.castro@test.com",
                "age": 29,
                "city": "Valencia",
                "role": "user",
                "status": "active",
                "created_at": "2025-07-28T17:00:00Z",
            },
            {
                "_id": "carlos_sanz",
                "name": "Carlos Sanz",
                "email": "carlos.sanz@outlook.com",
                "age": 50,
                "city": "Sevilla",
                "role": "user",
                "status": "active",
                "created_at": "2025-07-28T18:00:00Z",
            },
            {
                "_id": "adonay",
                "name": "Adonay",
                "email": "adonay@example.com",
                "age": 30,
                "city": "Madrid",
                "role": "admin",
                "status": "active",
                "created_at": "2025-07-28T19:00:00Z",
            },
        ]
        await client_admin.collection_item_set_many(query_collection_name, user_data)
        end_time = asyncio.get_event_loop().time()
        print(f"Query_Data_Preparation: {end_time - start_time:.3f}s")
        print("✔ Success: Data prepared for collection query tests.")

        try:
            # Query 1: Users with email in example.com, ordered by name ASC
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    filter={"field": "email", "op": "like", "value": "%@example.com"},
                    orderBy=[OrderByClause(field="name", direction="asc")],
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_1_EmailLike: {end_time - start_time:.3f}s")
            expected_names1 = [
                "Adonay",
                "David Blanco",
                "Juan Pérez",
                "María García",
                "Sofía Ruiz",
            ]
            if (
                len(query_result) == len(expected_names1)
                and [u["name"] for u in query_result] == expected_names1
            ):
                print("✔ Success: Query 1 result matches expected data and order.")
            else:
                print("✖ Error: Query 1 result mismatch.")
                print("Result:", [u["name"] for u in query_result])

            # Query 2: Users > 25 years old AND (admin OR moderator role)
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    filter={
                        "and": [
                            {"field": "age", "op": ">", "value": 25},
                            {
                                "or": [
                                    {"field": "role", "op": "=", "value": "admin"},
                                    {"field": "role", "op": "=", "value": "moderator"},
                                ],
                            },
                        ],
                    },
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_2_AgeRole: {end_time - start_time:.3f}s")
            expected_names2 = sorted(
                ["Adonay", "Pedro López", "Sofía Ruiz", "María García", "David Blanco"]
            )
            actual_names2 = sorted([u["name"] for u in query_result])
            if actual_names2 == expected_names2:
                print("✔ Success: Query 2 result matches expected data.")
            else:
                print("✖ Error: Query 2 result mismatch.")
                print("Result:", actual_names2)

            # Query 3: Count active users
            start_time = asyncio.get_event_loop().time()
            count_query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    count=True,
                    filter={"field": "status", "op": "=", "value": "active"},
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_3_CountActive: {end_time - start_time:.3f}s")
            if count_query_result.get("count") == 9:
                print(
                    f"✔ Success: Query 3 count matches expected count ({count_query_result.get('count')})."
                )
            else:
                print(
                    f"✖ Error: Query 3 count mismatch. Expected 9, got {count_query_result.get('count')}."
                )

            # Query 4: 5 most recent users (ordered by created_at DESC)
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    orderBy=[OrderByClause(field="created_at", direction="desc")],
                    limit=5,
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_4_MostRecent: {end_time - start_time:.3f}s")
            expected_names4 = [
                "Adonay",
                "Carlos Sanz",
                "Elena Castro",
                "David Blanco",
                "Sofía Ruiz",
            ]
            if (
                len(query_result) == len(expected_names4)
                and [u["name"] for u in query_result] == expected_names4
            ):
                print("✔ Success: Query 4 result matches expected data and order.")
            else:
                print("✖ Error: Query 4 result mismatch.")
                print("Result:", [u["name"] for u in query_result])

            # Query 5: Users named "Juan" or "María"
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    filter={
                        "or": [
                            {"field": "name", "op": "like", "value": "%Juan%"},
                            {"field": "name", "op": "like", "value": "%María%"},
                        ],
                    },
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_5_JuanMaria: {end_time - start_time:.3f}s")
            expected_names5 = sorted(["Juan Pérez", "María García"])
            actual_names5 = sorted([u["name"] for u in query_result])
            if actual_names5 == expected_names5:
                print("✔ Success: Query 5 result matches expected data.")
            else:
                print("✖ Error: Query 5 result mismatch.")
                print("Result:", actual_names5)

            # Query 6: Average age per city
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    aggregations={"average_age": Aggregation(func="avg", field="age")},
                    groupBy=["city"],
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_6_AvgAgePerCity: {end_time - start_time:.3f}s")
            if isinstance(query_result, list) and all(
                isinstance(item.get("city"), str)
                and isinstance(item.get("average_age"), (int, float))
                for item in query_result
            ):
                print("✔ Success: Query 6 result structure is as expected.")
            else:
                print("✖ Error: Query 6 result structure mismatch.")

            # Query 7: Users between 25 and 35 (inclusive)
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    filter={"field": "age", "op": "between", "value": [25, 35]},
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_7_AgeBetween: {end_time - start_time:.3f}s")
            expected_names7 = sorted(
                [
                    "Adonay",
                    "Juan Pérez",
                    "María García",
                    "Ana Díaz",
                    "Sofía Ruiz",
                    "Elena Castro",
                ]
            )
            actual_names7 = sorted([u["name"] for u in query_result])
            if actual_names7 == expected_names7:
                print("✔ Success: Query 7 result matches expected data.")
            else:
                print("✖ Error: Query 7 result mismatch.")
                print("Result:", actual_names7)

            # Query 8: Distinct cities
            start_time = asyncio.get_event_loop().time()
            query_result = await client_admin.collection_query(
                query_collection_name,
                Query(
                    distinct="city",
                ),
            )
            end_time = asyncio.get_event_loop().time()
            print(f"Query_8_DistinctCities: {end_time - start_time:.3f}s")
            expected_cities = sorted(
                ["Madrid", "Barcelona", "Valencia", "Sevilla", "Bilbao"]
            )
            actual_cities = sorted(query_result)
            if actual_cities == expected_cities:
                print("✔ Success: Query 8 result matches expected distinct cities.")
            else:
                print("✖ Error: Query 8 result mismatch.")
                print("Result:", actual_cities)

        except Exception as query_error:
            print(
                f"✖ Error: Failed to perform collectionQuery on '{query_collection_name}': {query_error}"
            )
        finally:
            # Clean up the dynamically created collection for query tests
            start_time = asyncio.get_event_loop().time()
            await client_admin.collection_delete(query_collection_name)
            end_time = asyncio.get_event_loop().time()
            print(f"Query_Data_Cleanup: {end_time - start_time:.3f}s")
            print(f"✔ Success: Collection '{query_collection_name}' deleted.")

        # 10. Delete an item
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_delete(test_collection_name, item_key1)
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_Delete: {end_time - start_time:.3f}s")
        print(f"✔ Success: Item '{item_key1}' deleted from '{test_collection_name}'.")

        # 11. Delete an item from the set many test
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_item_delete(test_collection_name, "many_item_c")
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_Delete_SetMany: {end_time - start_time:.3f}s")
        print(f"✔ Success: Item 'many_item_c' deleted from '{test_collection_name}'.")

        start_time = asyncio.get_event_loop().time()
        remaining_items = await client_admin.collection_item_list(test_collection_name)
        end_time = asyncio.get_event_loop().time()
        print(f"CollectionItem_List_AfterDelete: {end_time - start_time:.3f}s")
        remaining_keys = list(remaining_items.items.keys())
        # The original count was 5 (2 old + 3 new). We deleted many_item_a, many_item_b, item_from_python_1, and now many_item_c.
        # So only `item_from_python_2` should remain.
        if len(remaining_keys) == 1 and item_key2 in remaining_keys:
            print("✔ Success: Correct number of items remain after deletions.")
        else:
            print("✖ Error: Incorrect number of items remain after deletions.")
            print("Remaining keys:", remaining_keys)

        # 12. Delete the test collection
        start_time = asyncio.get_event_loop().time()
        await client_admin.collection_delete(test_collection_name)
        end_time = asyncio.get_event_loop().time()
        print(f"Collection_Delete: {end_time - start_time:.3f}s")
        print(f"✔ Success: Collection '{test_collection_name}' deleted.")

        # 13. List collections (after deletion)
        start_time = asyncio.get_event_loop().time()
        collections_after_delete = await client_admin.collection_list()
        end_time = asyncio.get_event_loop().time()
        print(f"Collection_List_AfterDelete: {end_time - start_time:.3f}s")
        if test_collection_name not in collections_after_delete.names:
            print(
                f"✔ Success: Collection '{test_collection_name}' not found in list after deletion."
            )
        else:
            print(
                f"✖ Error: Collection '{test_collection_name}' found in list after deletion unexpectedly."
            )

    except Exception as error:
        print("\n--- UNEXPECTED ERROR DURING TESTS ---")
        print(f"Error: {error}")
        if "Authentication failed" in str(error):
            print(
                "Hint: This error indicates the server did not recognize the authentication command or rejected it. Check the CMD_AUTHENTICATE value on both sides (Go and Python) and the UserInfo struct in Go for invisible characters."
            )
            print(
                "Also ensure your 'admin' user in the Go DB doesn't have 'IsRoot: true' by mistake."
            )
        elif "Connection refused" in str(error):
            print(
                "Hint: Is the MemoryTools Go server running and listening on the specified host and port?"
            )
    finally:
        print("\nClosing connection...")
        if client_admin:
            client_admin.close()
        print("--- All tests finished ---")


if __name__ == "__main__":
    asyncio.run(run_tests())
