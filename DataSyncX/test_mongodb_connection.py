from pymongo import MongoClient
import configparser
import os

def test_connection():
    try:
        # Load configuration
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'props.properties')
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Get MongoDB URI
        uri = config['DEFAULT']['mongodb_uri']
        
        # Try to connect
        print("Attempting to connect to MongoDB...")
        client = MongoClient(uri)
        
        # Test the connection
        client.admin.command('ping')
        print("✅ Successfully connected to MongoDB!")
        
        # Print databases (to verify access)
        print("\nAccessible databases:")
        for db in client.list_database_names():
            print(f"- {db}")
            
        return client
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

if __name__ == "__main__":
    test_connection() 