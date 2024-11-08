import streamlit as st
# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="DataSyncX Monitor",
    page_icon="📊",
    layout="wide"
)

import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import configparser
import os
from datetime import datetime, timedelta

# Load configuration first
def load_config():
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'props.properties')
        if not os.path.exists(config_path):
            st.error(f"Configuration file not found at: {config_path}")
            st.stop()
            
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    except Exception as e:
        st.error(f"Error loading configuration: {e}")
        st.stop()

# Load config before anything else
config = load_config()

# Initialize MongoDB connection
@st.cache_resource
def init_mongodb(_config):
    try:
        client = MongoClient(_config['DEFAULT']['mongodb_uri'])
        # Test connection
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return None

# Get MongoDB client and databases
client = init_mongodb(config)

if not client:
    st.error("Could not connect to MongoDB. Please check your connection settings.")
    st.stop()

# Initialize databases
try:
    db = client[config['DEFAULT']['mongodb_database']]
    log_db = client[config['DEFAULT']['mongodb_log_database']]
except Exception as e:
    st.error(f"Error accessing databases: {e}")
    st.stop()

# Custom CSS
st.markdown("""
    <style>
        .stMetric {
            background-color: #4CAF50;
            padding: 10px;
            border-radius: 5px;
            color: white;
        }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
        }
    </style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("Filters")

# Get unique activity IDs
try:
    activity_ids = list(log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].distinct('activity_id'))
    if activity_ids:
        selected_activity = st.sidebar.selectbox("Select Activity ID", activity_ids)
    else:
        st.sidebar.info("No activities found")
        selected_activity = None
except Exception as e:
    st.sidebar.error(f"Error loading activity IDs: {e}")
    selected_activity = None

# Date range filter
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=7), datetime.now())
)

# Main content
try:
    # Fetch data from MongoDB collections
    activity_info = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].find()))
    pair_history = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_pair_history_collection']].find()))
    s3_logs = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_s3_collection']].find()))

    # Display metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        try:
            total_docs = db[config['DEFAULT']['mongodb_collection']].count_documents({})
            st.metric("Total Documents", total_docs)
        except Exception as e:
            st.metric("Total Documents", "Error")
            st.error(f"Error counting documents: {e}")
    
    with col2:
        if not activity_info.empty:
            try:
                success_rate = (activity_info['passed_files'].sum() / activity_info['total_files'].sum() * 100) if activity_info['total_files'].sum() > 0 else 0
                st.metric("Success Rate", f"{success_rate:.2f}%")
            except Exception as e:
                st.metric("Success Rate", "Error")
                st.error(f"Error calculating success rate: {e}")
    
    with col3:
        try:
            pickup_folder = config['DEFAULT']['pickup_folder']
            if os.path.exists(pickup_folder):
                pickup_files = len([f for f in os.listdir(pickup_folder) if os.path.isfile(os.path.join(pickup_folder, f))])
                st.metric("Files in Pickup", pickup_files)
            else:
                st.metric("Files in Pickup", "Folder not found")
        except Exception as e:
            st.metric("Files in Pickup", "Error")
            st.error(f"Error counting pickup files: {e}")

    # Display charts
    if not activity_info.empty:
        st.subheader("Processing Statistics")
        
        try:
            # Total Files Processed
            fig1 = px.bar(activity_info, 
                         x='activity_id', 
                         y='total_files',
                         title='Total Files Processed per Activity')
            st.plotly_chart(fig1)

            # File Sizes
            fig2 = px.bar(activity_info, 
                         x='activity_id', 
                         y=['total_xml_size', 'total_pdf_size'],
                         title='File Sizes per Activity',
                         barmode='group')
            st.plotly_chart(fig2)
        except Exception as e:
            st.error(f"Error creating charts: {e}")

    # Display tables
    st.subheader("Recent Processing History")
    if not pair_history.empty:
        try:
            st.dataframe(pair_history)
        except Exception as e:
            st.error(f"Error displaying history: {e}")
    else:
        st.info("No processing history available")

except Exception as e:
    st.error(f"Error loading data: {e}")

# Add refresh button
if st.button("Refresh Data"):
    st.experimental_rerun()