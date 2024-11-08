from pymongo import MongoClient
import configparser
import os
import urllib.parse

def debug_mongodb_connection():
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'props.properties')
        print(f"1. Looking for config file at: {config_path}")
        
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Retrieve the URI and database names from the config file
        uri = config['DEFAULT']['mongodb_uri']
        main_db_name = config['DEFAULT'].get('mongodb_database', 'test')
        log_db_name = config['DEFAULT'].get('mongodb_log_database', 'test_log')
        
        # Print URI for verification (partially)
        print("\n2. Checking MongoDB URI:")
        print(f"URI starts with: {uri[:30]}...")
        
        # Check URI format
        if "mongodb+srv://" in uri:
            print("\n3. URI Format: mongodb+srv:// (using DNS seedlist)")
        else:
            print("\n3. URI Format: standard mongodb://")
        
        # Attempt connection
        print("\n4. Attempting connection...")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Ping the server
        print("\n5. Testing server connection...")
        client.admin.command('ping')
        print("✅ Server ping successful!")
        
        # List databases
        print("\n6. Listing available databases...")
        dbs = client.list_database_names()
        print("Available databases:", dbs)
        
        # Test specific database access
        print(f"\n7. Testing access to main database ({main_db_name})...")
        main_db = client[main_db_name]
        main_db.command('ping')
        print(f"✅ Can access {main_db_name}")
        
        print(f"\n8. Testing access to log database ({log_db_name})...")
        log_db = client[log_db_name]
        log_db.command('ping')
        print(f"✅ Can access {log_db_name}")
        
        return True
    
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        print("\nDetailed error information:")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("=== MongoDB Connection Debugging ===")
    success = debug_mongodb_connection()
    if success:
        print("\n✅ All connection tests passed successfully!")
    else:
        print("\n❌ Connection testing failed. Please check the error messages above.")
