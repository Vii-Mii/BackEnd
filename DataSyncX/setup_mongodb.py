from pymongo import MongoClient
import configparser
import os

def setup_mongodb():
    # Load configuration
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'props.properties')
    config = configparser.ConfigParser()
    config.read(config_path)

    # Connect to MongoDB
    client = MongoClient(config['DEFAULT']['mongodb_uri'])
    
    try:
        # Create main database and collection
        main_db = client[config['DEFAULT']['mongodb_database']]
        main_db.create_collection(config['DEFAULT']['mongodb_collection'])
        print(f"Created main database: {config['DEFAULT']['mongodb_database']}")
        print(f"Created main collection: {config['DEFAULT']['mongodb_collection']}")

        # Create logs database and collections
        log_db = client[config['DEFAULT']['mongodb_log_database']]
        
        log_collections = [
            'mongodb_log_activity_info_collection',
            'mongodb_log_pair_history_collection',
            'mongodb_log_s3_collection',
            'mongodb_log_History_collection'
        ]

        for collection in log_collections:
            log_db.create_collection(config['DEFAULT'][collection])
            print(f"Created log collection: {config['DEFAULT'][collection]}")

        # Create indexes for better query performance
        main_db[config['DEFAULT']['mongodb_collection']].create_index('dhr_id')
        log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].create_index('activity_id')
        log_db[config['DEFAULT']['mongodb_log_pair_history_collection']].create_index('activity_id')
        log_db[config['DEFAULT']['mongodb_log_s3_collection']].create_index('activity_id')
        
        print("\nMongoDB setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up MongoDB: {e}")

if __name__ == "__main__":
    setup_mongodb() 