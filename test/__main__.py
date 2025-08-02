import asyncio
import logging
from src.memory_tools_client import MemoryToolsClient, Query

# --- Configuración ---
# Cambia estas variables si tu servidor tiene una configuración diferente
HOST = "127.0.0.1"
PORT = 5876
USER = "admin"
PASS = "adminpass"
CERT_PATH = None  # o la ruta a tu 'server.crt' si usas verificación estricta
REJECT_UNAUTHORIZED = False # Poner en True si usas un certificado válido

# Configura un logging simple para mantener la salida limpia
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


async def run_tests():
    """
    Ejecuta una suite completa de pruebas para MemoryToolsClient.
    Asegúrate de que el servidor de Go esté en ejecución antes de lanzar el test.
    """
    print("🚀 Starting Memory Tools Python client tests...")

    # --- Prueba 1: Falla de Autenticación ---
    print("\n--- Testing Failed Authentication ---")
    try:
        # El 'async with' maneja la conexión y el cierre automáticamente
        async with MemoryToolsClient(HOST, PORT, "bad_user", "bad_pass", reject_unauthorized=REJECT_UNAUTHORIZED):
             print("✖ Error: Authentication with incorrect credentials unexpectedly succeeded.")
    except Exception as e:
        print(f"✔ Success: Authentication failed as expected: {e}")

    # --- Suite de Pruebas Principales ---
    print("\n--- Starting Main Test Suite ---")
    try:
        async with MemoryToolsClient(HOST, PORT, USER, PASS, CERT_PATH, REJECT_UNAUTHORIZED) as client:
            print(f"✔ Success: Connected and authenticated as '{client.authenticated_user}'.")
            if not client.is_authenticated:
                raise Exception("`is_authenticated` flag was not set correctly.")
            print("✔ Success: `is_authenticated` returned True.")

            # --- Pruebas de la Tienda Principal (Key-Value) ---
            print("\n--- Testing Main Store Operations ---")
            key, value = "py_key", {"app": "Python Test", "version": 1.0}
            await client.set(key, value, ttl_seconds=60)
            print(f"✔ Success: SET key '{key}'.")
            
            retrieved = await client.get(key)
            if not (retrieved.found and retrieved.value == value):
                raise Exception("GET verification failed for main store.")
            print(f"✔ Success: GET verified value for key '{key}'.")
            
            await client.set(key, None, 0) # La eliminación se simula con valor None y TTL 0
            print(f"✔ Success: Key '{key}' deleted from main store.")

            # --- Pruebas del Ciclo de Vida de una Colección ---
            print("\n--- Testing Full Collection Lifecycle ---")
            coll_name = "py_lifecycle_coll"
            
            # 1. Creación
            await client.collection_create(coll_name)
            print(f"✔ Success: CREATED collection '{coll_name}'.")
            
            collections = await client.collection_list()
            if coll_name not in collections:
                raise Exception(f"'{coll_name}' not found after creation.")
            print(f"✔ Success: LIST verified collection exists.")

            # 2. Población de Datos
            item1 = {"_id": "user:1", "city": "Madrid", "active": True}
            item2 = {"_id": "user:2", "city": "Barcelona", "active": True}
            many_items = [
                {"_id": "user:3", "city": "Madrid", "active": False},
                {"_id": "user:4", "city": "Valencia", "active": True}
            ]

            await client.collection_item_set(coll_name, item1["_id"], item1)
            print(f"✔ Success: SET single item 'user:1'.")
            
            retrieved_item = await client.collection_item_get(coll_name, item1["_id"])
            if not (retrieved_item.found and retrieved_item.value == item1):
                raise Exception("collection_item_get verification failed.")
            print(f"✔ Success: GET verified single item 'user:1'.")

            await client.collection_item_set_many(coll_name, [item2] + many_items)
            print("✔ Success: SET MANY items.")

            # 3. Operaciones de Índices
            await client.collection_index_create(coll_name, "city")
            print("✔ Success: CREATED index on 'city'.")

            indexes = await client.collection_index_list(coll_name)
            if "city" not in indexes:
                raise Exception("Index 'city' not found in list.")
            print(f"✔ Success: LIST verified index exists: {indexes}")
                
            # 4. Pruebas de Consultas (Query)
            active_madrid_users = await client.collection_query(coll_name, Query(
                filter={"and": [{"field": "city", "op": "=", "value": "Madrid"}, {"field": "active", "op": "=", "value": True}]}
            ))
            if len(active_madrid_users) != 1 or active_madrid_users[0]["_id"] != "user:1":
                raise Exception("Query result is incorrect.")
            print("✔ Success: QUERY returned correct filtered data.")

            count_result = await client.collection_query(coll_name, Query(count=True))
            if count_result.get("count") != 4:
                raise Exception(f"Count query failed, got {count_result.get('count')}")
            print(f"✔ Success: COUNT query returned correct count: {count_result['count']}")

            # 5. Limpieza (Cleanup)
            await client.collection_item_delete(coll_name, "user:2")
            print("✔ Success: DELETED single item 'user:2'.")
            
            await client.collection_item_delete_many(coll_name, ["user:3", "user:4"])
            print("✔ Success: DELETED MANY items.")
            
            final_items = await client.collection_item_list(coll_name)
            if len(final_items) != 1 or "user:1" not in final_items:
                raise Exception(f"Incorrect items remain after deletions: {list(final_items.keys())}")
            print("✔ Success: LIST verified correct items remain.")

            await client.collection_index_delete(coll_name, "city")
            print("✔ Success: DELETED index on 'city'.")
            
            await client.collection_delete(coll_name)
            print(f"✔ Success: DELETED collection '{coll_name}'.")

    except Exception as e:
        print(f"\n--- 💥 UNEXPECTED ERROR DURING TESTS 💥 ---")
        print(f"Error: {e}")
        if isinstance(e, ConnectionRefusedError):
            print("Hint: Is the Go server running? Use: `go run ./cmd/server`")
    finally:
        print("\n--- All tests finished ---")

if __name__ == "__main__":
    asyncio.run(run_tests())