import requests
import pyodbc
import json
import time
from datetime import datetime
import schedule







# Base API URLs
BASE_URL = "https://example.net/api"
AUTH_URL = f"{BASE_URL}/Authorization/GetToken/"
CREATE_UPDATE_PRODUCT_URL = f"{BASE_URL}/CreateUpdateProduct/"
CREATE_UPDATE_STOCK_URL = f"{BASE_URL}/CreateUpdateStock/"
CREATE_UPDATE_PRICE_URL = f"{BASE_URL}/CreateUpdatePrice/"
ORDER_URL = f"{BASE_URL}/GetOrders/"





# Database connection setup 
DB_CONNECTION = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=OnlineAPI;UID=test;PWD=test"
LOG_FILE = "log.txt"

# Log error messages to a file
def log_error(message):
    """Log errors to a file with timestamp.""" 
    with open(LOG_FILE, "a") as log:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log.write(f"[{timestamp}] {message}\n")

# Retry failed functions with exponential backoff
def retry_request(func, max_retries=5, delay=2, *args, **kwargs):
    """Retry a function up to a maximum number of retries."""
    for attempt in range(max_retries): 
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(delay * (2 ** attempt))  # Exponential backoff
                print(f"Retrying... Attempt {attempt + 2}")
            else:
                log_error(f"Maximum retries reached. Error: {e}")
                print(f"Maximum retries reached. Error: {e}")
                return None





# Fetch token from database for a specific location
def get_static_token(location):
    """
    Retrieve the static token for a given location from the database.
    """
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()

        # Fetch the token for the specific location
        cursor.execute("""
            SELECT Token
            FROM UsersTableSHOPAZ
            WHERE location = ?
        """, location)
        result = cursor.fetchone()
        connection.close()

        if result and result[0]:
            return result[0]
        else:
            print(f"No token found for location: {location}")
            return None
    except Exception as e:
        message = f"Failed to fetch token for location: {location}. Error: {e}"
        log_error(message)
        print(message)
        return None






# Fetch items to be updated from ProductsSHOPAZ table
def fetch_items_from_db():
    """Fetch items from the ProductsSHOPAZ."""
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM ProductsSHOPAZ WHERE changeFlag = 0")
        rows = cursor.fetchall()
        items = []
        for row in rows:
            try:
                item = {
                    "externalId": row[0],
                    "name": row[1],
                    "description": row[2],
                    "taxCode": row[3],
                    "attributes": json.loads(row[4]),
                    "brand": row[5],
                    "categories": json.loads(row[6]),
                    "images": json.loads(row[7]),
                    "skus": json.loads(row[8])
                }
                # Ensure location is present
                if row[10]:  
                    items.append((row[0], row[10], item))  # Include ExternalId and Location
                else:
                    log_error(f"Item {row[0]} does not have a valid location.")
            except Exception as e:
                log_error(f"Failed to process item {row[0]}: {e}")
        connection.close()
        return items
    except Exception as e:
        message = f"Database fetch failed: {e}"
        log_error(message)
        print(message)
        return []





# Post items to CreateUpdateProduct endpoint
def create_update_products():
    """Post items to CreateUpdateProduct endpoint grouped by location."""
    items = fetch_items_from_db()
    grouped_items = {}
    for external_id, location, item in items:
        grouped_items.setdefault(location, []).append((external_id, item))

    for location, location_items in grouped_items.items():
        # Fetch the static token for the location
        token = get_static_token(location)
        if not token:
            log_error(f"No valid token for location: {location}")
            print(f"No valid token for location: {location}")
            continue

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        filtered_items = [(external_id, item) for external_id, item in location_items if item.get("images")]

        if not filtered_items:
            print(f"No items with non-empty images to process for location: {location}")
            continue

        def request_func(item):
            response = requests.post(CREATE_UPDATE_PRODUCT_URL, json=[item], headers=headers, timeout=30)
            response.raise_for_status()
            return response

        for external_id, item in filtered_items:
            response = retry_request(request_func, item=item)
            if response and response.status_code == 200:
                print(f"Item {external_id} posted successfully.")
            else:
                log_error(f"Failed to post item {external_id}.")





# Fetch stock updates from database
def fetch_stock_updates_from_db():
    """Fetch stock updates from the database along with location."""
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()
        cursor.execute("SELECT skuId, quantity, vtexWarehouseId, location FROM StockTableSHOPAZ WHERE changeFlag = 0")
        rows = cursor.fetchall()
        items = [
            {"skuId": row[0], "quantity": row[1], "vtexWarehouseId": row[2], "location": row[3]}
            for row in rows
        ]
        connection.close()
        return items
    except Exception as e:
        message = f"Database fetch for stock updates failed: {e}"
        log_error(message)
        print(message)
        return []





# Post stock updates to CreateUpdateStock endpoint
def update_stock():
    """Post stock updates to CreateUpdateStock endpoint grouped by location."""
    stock_items = fetch_stock_updates_from_db()
    grouped_by_location = {}
    for item in stock_items:
        location = item["location"]
        grouped_by_location.setdefault(location, []).append(item)

    for location, items in grouped_by_location.items():
        # Fetch the static token for the location
        token = get_static_token(location)
        if not token:
            log_error(f"No valid token for location: {location}")
            print(f"No valid token for location: {location}")
            continue

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        def request_func():
            response = requests.post(CREATE_UPDATE_STOCK_URL, json=items, headers=headers, timeout=30)
            response.raise_for_status()
            return response

        response = retry_request(request_func)
        if response and response.status_code == 200:
            print(f"Stock items posted successfully for location {location}.")
        else:
            log_error(f"Failed to post stock items for location {location}.")





def fetch_price_updates_from_db():
    """Fetch price updates from the database along with location."""
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT skuId, price, discountPrice, minQuantity, discountMinQuantity, fromDate, toDate, location
            FROM PriceTableSHOPAZ
            WHERE changeFlag = 0
        """)
        rows = cursor.fetchall()
        items = [
            {
                "skuId": row[0],
                "price": {
                    "price": row[1],
                    "discountPrice": row[2],
                    "minQuantity": row[3],
                    "discountMinQuantity": row[4],
                    "fromDate": row[5].strftime("%Y-%m-%dT%H:%M:%S"),
                    "toDate": row[6].strftime("%Y-%m-%dT%H:%M:%S")
                },
                "location": row[7]
            }
            for row in rows
        ]
        connection.close()
        return items
    except Exception as e:
        message = f"Database fetch for price updates failed: {e}"
        log_error(message)
        print(message)
        return []





def update_price_flag(sku_id):
    """Update the changeFlag for a specific price record."""
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()
        cursor.execute("""
            UPDATE PriceTableSHOPAZ
            SET changeFlag = 1
            WHERE skuId = ?
        """, sku_id)
        connection.commit()
        connection.close()
        print(f"Successfully updated changeFlag for skuId: {sku_id}")
    except Exception as e:
        message = f"Failed to update changeFlag for skuId: {sku_id}. Error: {e}"
        log_error(message)
        print(message)





def update_price(price_items):
    """Post price updates to CreateUpdatePrice endpoint grouped by location."""
    grouped_by_location = {}
    for item in price_items:
        location = item["location"]
        grouped_by_location.setdefault(location, []).append(item)

    for location, items in grouped_by_location.items():
        # Fetch the static token for the location
        token = get_static_token(location)
        if not token:
            log_error(f"No valid token for location: {location}")
            print(f"No valid token for location: {location}")
            continue

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = [{"skuId": i["skuId"], "price": i["price"]} for i in items]

        def request_func():
            response = requests.post(CREATE_UPDATE_PRICE_URL, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            return response

        response = retry_request(request_func)
        if response and response.status_code == 200:
            print(f"Price items posted successfully for location {location}.")
            for item in items:
                update_price_flag(item["skuId"])
        else:
            log_error(f"Failed to post price items for location {location}.")





def fetch_and_insert_orders():
    """Fetch orders from API for each location and insert into OrdersTableSHOPAZ."""
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()
        
        # Fetch all locations from the UsersTableSHOPAZ
        cursor.execute("SELECT location FROM UsersTableSHOPAZ")
        locations = [row[0] for row in cursor.fetchall()]
        connection.close()

        for location in locations:
            # Fetch the static token for the location
            token = get_static_token(location)
            if not token:
                log_error(f"No valid token for location: {location}")
                print(f"No valid token for location: {location}")
                continue

            # Define the request payload
            payload = {
                "echo": "get_all_orders",
                "search": "",
                "displayLength": 1000,
                "displayStart": 0,
                "sortCol": 0,
                "sortDir": "desc",
                "sortingCols": 1,
                "sColumns": "",
                "status":"",
                "includeDetails": True
            }
            
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            
            try:
                # Send API request to GetOrders endpoint
                response = requests.post(ORDER_URL, json=payload, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Log the response for debugging
                print(f"API Response for location {location}: {data}")

                # Process the API response
                orders = data.get("Data", [])
                if not orders:
                    print(f"No orders found for location {location}.")
                    continue
                
                connection = pyodbc.connect(DB_CONNECTION)
                cursor = connection.cursor()
                
                # Enable IDENTITY_INSERT for manual Id insertion
                cursor.execute("SET IDENTITY_INSERT OrdersTableSHOPAZ ON")
                
                for order in orders:
                    # Extract necessary data
                    record_id = order["Id"]
                    order_date = order["CreateDate"]
                    order_id = order["OrderId"]
                    quantity = order["OrderDetails"][0]["Quantity"]
                    price = order["OrderDetails"][0]["UnitPrice"]
                    item_no = order["OrderDetails"][0]["ProductNo"]
                    item_description = order["OrderDetails"][0]["ProductDescription"]
                    posting_description = f'{order["RecipientName"]}, {order["RecipientCity"]}, {order["RecipientPhone"]}'
                    change_flag = 0
                    document_type = 2 if order["Status"] == "cancellation-requested" else 0

                    # Check if the Id already exists in the database
                    cursor_check = connection.cursor()
                    cursor_check.execute("SELECT COUNT(*) FROM OrdersTableSHOPAZ WHERE Id = ?", record_id)
                    exists = cursor_check.fetchone()[0]

                    if exists > 0:
                        # Skip if the record already exists
                        print(f"Record with Id={record_id} already exists. Skipping insertion.")
                        continue

                    # Print data being prepared for insertion
                    print(f"Inserting into DB: Id={record_id}, Location={location}, OrderDate={order_date}, "
                          f"OrderId={order_id}, Quantity={quantity}, Price={price}, "
                          f"ItemNo={item_no}, ItemDescription={item_description}, "
                          f"PostingDescription={posting_description}, ChangeFlag={change_flag}, "
                          f"DocumentType={document_type}")
                    
                    # Insert data into OrdersTableSHOPAZ
                    cursor.execute("""
                        INSERT INTO OrdersTableSHOPAZ 
                        (Id, Location, OrderDate, OrderId, Quantity, Price, ItemNo, ItemDescription, PostingDescription, ChangeFlag, DocumentType)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, record_id, location, order_date, order_id, quantity, price, item_no, item_description, posting_description, change_flag, document_type)
                
                # Disable IDENTITY_INSERT after insertion
                cursor.execute("SET IDENTITY_INSERT OrdersTableSHOPAZ OFF")
                
                connection.commit()
                connection.close()
                print(f"Successfully inserted orders for location: {location}")
            except Exception as e:
                log_error(f"Failed to fetch or insert orders for location {location}. Error: {e}")
                print(f"Failed to process orders for location {location}. Error: {e}")
    except Exception as e:
        log_error(f"Error in fetch_and_insert_orders function: {e}")
        print(f"Error in fetch_and_insert_orders function: {e}")





#Funksionet per porosite
def start_order_handling():
    """
    Fetch OrderId records from OrdersTableSHOPAZ with ChangeFlag = 1 and OrderStatus = 'Pranuar',
    then send GET requests to the /Sync/StartOrderHandling endpoint for each OrderId.
    """
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()

        # Fetch records from the OrdersTableSHOPAZ
        cursor.execute("""
            SELECT DISTINCT OrderId, location
            FROM OrdersTableSHOPAZ
            WHERE ChangeFlag = 1 AND OrderStatus = 'Pranuar'
        """)
        rows = cursor.fetchall()
        connection.close()

        if not rows:
            print("No orders with ChangeFlag = 1 and OrderStatus = 'Pranuar' found.")
            return

        for order_id, location in rows:
            # Fetch the static token for the location
            token = get_static_token(location)
            if not token:
                log_error(f"No valid token for location: {location}")
                print(f"No valid token for location: {location}")
                continue

            headers = {"Authorization": f"Bearer {token}"}
            url = f"{BASE_URL}/Sync/StartOrderHandling?orderId={order_id}"

            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                # If successful, update the ChangeFlag in the database
                print(f"Successfully processed OrderId: {order_id}")
                connection = pyodbc.connect(DB_CONNECTION)
                cursor = connection.cursor()
                cursor.execute("""
                    UPDATE OrdersTableSHOPAZ
                    SET ChangeFlag = 1
                    WHERE OrderId = ?
                """, order_id)
                connection.commit()
                connection.close()
            except requests.exceptions.RequestException as e:
                log_error(f"Failed to process OrderId: {order_id}. Error: {e}")
                print(f"Failed to process OrderId: {order_id}. Error: {e}")
    except Exception as e:
        log_error(f"Error in start_order_handling function: {e}")
        print(f"Error in start_order_handling function: {e}")





# Function to finish order 
def generate_invoice():
    """
    Fetch distinct OrderId records from OrdersTableSHOPAZ with ChangeFlag = 1 and OrderStatus = 'READY',
    then send GET requests to the /Sync/GenerateInvoice endpoint for each OrderId.
    """
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()

        # Fetch distinct records from the OrdersTableSHOPAZ
        cursor.execute("""
            SELECT DISTINCT OrderId, location
            FROM OrdersTableSHOPAZ
            WHERE ChangeFlag = 1 AND OrderStatus = 'READY'
        """)
        rows = cursor.fetchall()
        connection.close()

        if not rows:
            print("No orders with ChangeFlag = 1 and OrderStatus = 'READY' found.")
            return

        for order_id, location in rows:
            # Fetch the static token for the location
            token = get_static_token(location)
            if not token:
                log_error(f"No valid token for location: {location}")
                print(f"No valid token for location: {location}")
                continue

            headers = {"Authorization": f"Bearer {token}"}
            url = f"{BASE_URL}/Sync/GenerateInvoice?orderId={order_id}"

            try:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                # If successful, update the ChangeFlag in the database to 2
                print(f"Successfully generated invoice for OrderId: {order_id}")
                connection = pyodbc.connect(DB_CONNECTION)
                cursor = connection.cursor()
                cursor.execute("""
                    UPDATE OrdersTableSHOPAZ
                    SET OrderStatus = 'Done'
                    WHERE OrderId = ?
                """, order_id)
                connection.commit()
                connection.close()
            except requests.exceptions.RequestException as e:
                log_error(f"Failed to generate invoice for OrderId: {order_id}. Error: {e}")
                print(f"Failed to generate invoice for OrderId: {order_id}. Error: {e}")
    except Exception as e:
        log_error(f"Error in generate_invoice function: {e}")
        print(f"Error in generate_invoice function: {e}")





def cancel_order():
    """
    Fetch records from OrdersTableSHOPAZ with ChangeFlag = 1, OrderStatus = 'CANCELLED', and Reason not null,
    then send GET requests to the /Sync/CancelOrder endpoint twice for each OrderId to confirm cancellation.
    """
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()

        # Fetch records that meet the specified criteria
        cursor.execute("""
            SELECT DISTINCT OrderId, location, Reason
            FROM OrdersTableSHOPAZ
            WHERE ChangeFlag = 1 AND OrderStatus = 'CANCELLED' AND Reason IS NOT NULL AND Reason <> ''
        """)
        rows = cursor.fetchall()
        connection.close()

        if not rows:
            print("No orders found with ChangeFlag = 1, OrderStatus = 'CANCELLED', and valid Reason.")
            return

        for order_id, location, reason in rows:
            # Fetch the static token for the location
            token = get_static_token(location)
            if not token:
                log_error(f"No valid token for location: {location}")
                print(f"No valid token for location: {location}")
                continue

            headers = {"Authorization": f"Bearer {token}"}
            url = f"{BASE_URL}/Sync/CancelOrder?orderId={order_id}&reason={reason.replace(' ', '+')}"

            # Send the first GET request
            try:
                response_1 = requests.get(url, headers=headers, timeout=30)
                response_1.raise_for_status()
                print(f"First cancellation request successful for OrderId: {order_id}")

                # Send the second GET request for confirmation
                response_2 = requests.get(url, headers=headers, timeout=30)
                response_2.raise_for_status()
                print(f"Second confirmation request successful for OrderId: {order_id}")

                # If successful, update the ChangeFlag in the database to 2
                connection = pyodbc.connect(DB_CONNECTION)
                cursor = connection.cursor()
                cursor.execute("""
                    UPDATE OrdersTableSHOPAZ
                    SET OrderStatus = 'Done'
                    WHERE OrderId = ?
                """, order_id)
                connection.commit()
                connection.close()
                print(f"OrderStatus updated to 2 for OrderId: {order_id}")
            except requests.exceptions.RequestException as e:
                log_error(f"Failed to process cancellation for OrderId: {order_id}. Error: {e}")
                print(f"Failed to process cancellation for OrderId: {order_id}. Error: {e}")
    except Exception as e:
        log_error(f"Error in cancel_order function: {e}")
        print(f"Error in cancel_order function: {e}")





def get_sticker_report():
    """
    Fetch distinct OrderId records with Sticker = NULL or empty, 
    then send GET requests to the /GetStickerReport endpoint for each OrderId 
    and update the Sticker field with the Base64 response.
    """
    try:
        connection = pyodbc.connect(DB_CONNECTION)
        cursor = connection.cursor()

        # Fetch distinct OrderId where Sticker is NULL or empty
        cursor.execute("""
            SELECT DISTINCT OrderId, location
            FROM OrdersTableSHOPAZ
            WHERE Sticker IS NULL OR Sticker = '' AND OrderStatus = 'READY'
        """)
        rows = cursor.fetchall()
        connection.close()

        if not rows:
            print("No orders found with Sticker NULL or empty.")
            return

        for order_id, location in rows:
            # Fetch the static token for the location
            token = get_static_token(location)
            if not token:
                log_error(f"No valid token for location: {location}")
                print(f"No valid token for location: {location}")
                continue

            headers = {"Authorization": f"Bearer {token}"}
            url = f"{BASE_URL}/GetStickerReport?orderId={order_id}"

            try:
                # Send GET request to fetch the sticker report
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()

                # Extract the Base64 data from the response
                base64_text = response.text  # Assuming the API returns plain Base64 string
                print(f"Successfully fetched sticker report for OrderId: {order_id}")

                # Update the Sticker field in the database
                connection = pyodbc.connect(DB_CONNECTION)
                cursor = connection.cursor()
                cursor.execute("""
                    UPDATE OrdersTableSHOPAZ
                    SET Sticker = ?
                    WHERE OrderId = ?
                """, base64_text, order_id)
                connection.commit()
                connection.close()

                print(f"Sticker updated successfully for OrderId: {order_id}")
            except requests.exceptions.RequestException as e:
                log_error(f"Failed to fetch sticker report for OrderId: {order_id}. Error: {e}")
                print(f"Failed to fetch sticker report for OrderId: {order_id}. Error: {e}")
            except Exception as e:
                log_error(f"Failed to update Sticker for OrderId: {order_id}. Error: {e}")
                print(f"Failed to update Sticker for OrderId: {order_id}. Error: {e}")
    except Exception as e:
        log_error(f"Error in get_sticker_report function: {e}")
        print(f"Error in get_sticker_report function: {e}")




# Call functions manually at the start
# create_update_products()
# update_stock()
# def update_price_scheduled():
#     price_items = fetch_price_updates_from_db()
#     update_price(price_items)

# schedule.every(30).minutes.do(update_price_scheduled)
# fetch_and_insert_orders()

# start_order_handling()
#generate_invoice()
# cancel_order()
# get_sticker_report()



# Schedule the functions to run every 30 minutes
schedule.every(30).minutes.do(create_update_products)
schedule.every(30).minutes.do(update_stock)
schedule.every(30).minutes.do(update_price)
schedule.every(30).minutes.do(fetch_and_insert_orders)
schedule.every(30).minutes.do(start_order_handling)
schedule.every(30).minutes.do(generate_invoice)
schedule.every(30).minutes.do(cancel_order)

print("Scheduled functions to run every 30 minutes.")

# Keep the script running to execute the schedule
while True:
    #schedule.run_pending()

    time.sleep(1)
