import streamlit as st
import pandas as pd
import plotly.express as px
import pymongo

# Initialize MongoDB client
client = "mongodb+srv://vijaymiiyath4300:ATIwRvZnqu5d92S6@cluster0.0ae4h.mongodb.net/"
db = client["datasyncx_db"]

# Enable wide mode and apply a custom theme
st.set_page_config(layout="wide", page_title="DataSyncX Dashboard", page_icon="📊")

# Custom CSS for enhanced styling
st.markdown("""
    <style>
        /* Global Font and Theme */
        body {
            font-family: 'Arial', sans-serif;
            background-color: #f7f7f7;
        }
        .main-content {
            padding: 20px;
            background-color: #ffffff;
            border-radius: 10px;
            box-shadow: 0px 4px 12px rgba(0, 0, 0, 0.1);
        }
        .sidebar .sidebar-content {
            background-color: #ffffff;
            border-right: 1px solid #e0e0e0;
            padding-top: 20px;
        }
        .stButton>button {
            background-color: #4CAF50;
            color: white;
            border: none;
            border-radius: 5px;
            padding: 10px 20px;
            margin: 10px 0;
            cursor: pointer;
        }
        .stButton>button:hover {
            background-color: #45a049;
        }
        /* Metrics Box Styling */
        .metric-box {
            background-color: #e9f0f7;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 10px;
            box-shadow: 0px 4px 6px rgba(0, 0, 0, 0.1);
            text-align: center;
        }
        .metric-box h3 {
            margin: 0;
            font-size: 20px;
            color: #2b2b2b;
        }
        .metric-box .value {
            font-size: 36px;
            font-weight: bold;
            color: #1e1e1e;
        }
        /* Navbar Styling */
        .navbar {
            background-color: #4CAF50;
            color: white;
            padding: 10px;
            border-radius: 5px;
            margin-bottom: 20px;
            text-align: center;
            font-size: 24px;
        }
        /* Footer Styling */
        .footer {
            text-align: center;
            padding: 10px;
            margin-top: 20px;
            border-top: 1px solid #e0e0e0;
            color: #888;
        }
        /* Tooltip Hover Effect */
        .stTooltip {
            background-color: #4CAF50;
            color: white;
            border-radius: 5px;
            padding: 5px;
        }
    </style>
""", unsafe_allow_html=True)

# Navbar
st.markdown('<div class="navbar">DataSyncX Admin Dashboard</div>', unsafe_allow_html=True)

# Sidebar filters in the first column
col1, col2, col3 = st.columns([0.1, 2.5, 1])

with col1:
    st.sidebar.title("⚙️ Filters")

    st.sidebar.subheader("🔍 Pair Events Filters")
    pair_event_activity_id = st.sidebar.text_input("🆔 Activity ID for Pair Events")
    pair_event_date_range = st.sidebar.date_input("📅 Date Range for Pair Events", [])

    st.sidebar.subheader("🗝️ S3 Keys Filters")
    s3_key_activity_id = st.sidebar.text_input("🆔 Activity ID for S3 Keys")
    s3_key_date_range = st.sidebar.date_input("📅 Date Range for S3 Keys", [])

    st.sidebar.subheader("📜 Pair History Filters")
    pair_history_activity_id = st.sidebar.text_input("🆔 Activity ID for Pair History")
    pair_history_date_range = st.sidebar.date_input("📅 Date Range for Pair History", [])

# Main content (Graphs and Tables) in the second column
with col2:
    st.header("Monitoring and Management")

    # Splitting the second column into three sub-columns for the graphs
    graph_col1, graph_col2, graph_col3 = st.columns(3)

    activity_data = list(db.activity_info.find())
    df_activity = pd.DataFrame(activity_data)

    if not df_activity.empty:
        # Total Files Processed per Activity (Graph 1)
        with graph_col1:
            fig_files = px.line(
                df_activity,
                x='activity_id',
                y='total_files',
                labels={"activity_id": "Activity ID", "total_files": "Total Files Processed"},
                title="📁 Total Files Processed per Activity",
                line_shape="linear",
            )
            fig_files.update_traces(line=dict(color='pink'))
            fig_files.update_xaxes(type='category')
            st.plotly_chart(fig_files, use_container_width=True)

        # Total File Size per Activity (Graph 2)
        with graph_col2:
            fig_size = px.line(
                df_activity,
                x='activity_id',
                y=['total_xml_size', 'total_pdf_size'],
                labels={"activity_id": "Activity ID", "value": "File Size (bytes)", "variable": "File Type"},
                title="📂 Total File Size per Activity"
            )
            fig_size.update_xaxes(type='category')
            st.plotly_chart(fig_size, use_container_width=True)

        # Processing Time per Activity (Graph 3)
        with graph_col3:
            df_activity['processing_time'] = pd.to_datetime(df_activity['activity_end_time']) - pd.to_datetime(df_activity['activity_start_time'])
            df_activity['processing_time'] = df_activity['processing_time'].dt.total_seconds()

            fig_time = px.line(
                df_activity,
                x='activity_id',
                y='processing_time',
                labels={"activity_id": "Activity ID", "processing_time": "Processing Time (seconds)"},
                title="⏱️ Processing Time per Activity",
                line_shape="linear",
            )
            fig_time.update_traces(line=dict(color='red'))
            fig_time.update_xaxes(type='category')
            st.plotly_chart(fig_time, use_container_width=True)
    else:
        st.write("No activity data available.")

    # Pair Events (Tabular Format with Filters)
    st.subheader("📋 Pair Events")
    query = {}
    if pair_event_activity_id:
        query['activity_id'] = pair_event_activity_id
    if len(pair_event_date_range) == 2:
        query['event_time'] = {"$gte": str(pair_event_date_range[0]), "$lte": str(pair_event_date_range[1])}

    pair_events_data = list(db.pair_history.find(query))
    df_pair_events = pd.DataFrame(pair_events_data)
    st.dataframe(df_pair_events)

    # S3 Keys (Tabular Format with Filters)
    st.subheader("🔑 S3 Keys")
    query = {}
    if s3_key_activity_id:
        query['activity_id'] = s3_key_activity_id
    if len(s3_key_date_range) == 2:
        query['s3_upload_time'] = {"$gte": str(s3_key_date_range[0]), "$lte": str(s3_key_date_range[1])}

    s3_key_data = list(db.pair_s3_key.find(query))
    df_s3_key = pd.DataFrame(s3_key_data)
    st.dataframe(df_s3_key)

    # Pair History (Tabular Format with Filters)
    st.subheader("📜 Pair History")
    query = {}
    if pair_history_activity_id:
        query['activity_id'] = pair_history_activity_id
    if len(pair_history_date_range) == 2:
        query['history_time'] = {"$gte": str(pair_history_date_range[0]), "$lte": str(pair_history_date_range[1])}

    pair_history_data = list(db.pair_status.find(query))
    df_pair_history = pd.DataFrame(pair_history_data)
    st.dataframe(df_pair_history)

# Total XML and PDF document count in the third column
with col3:
    st.subheader("📝 Document Counts")

    # Count total documents in the pair_history collection
    total_document_count = db.pair_history.count_documents({})

    # Assign total document count to both XML and PDF metrics
    total_xml_count = total_document_count
    total_pdf_count = total_document_count

    # Display counts in a minimalist and shaded component style
    st.markdown(f"""
    <div class="metric-box">
        <h3>Total XML Files</h3>
        <div class="value">{total_xml_count}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="metric-box">
        <h3>Total PDF Files</h3>
        <div class="value">{total_pdf_count}</div>
    </div>
    """, unsafe_allow_html=True)

    # Add an example of another metric
    st.markdown(f"""
    <div class="metric-box">
        <h3>Current Files in Pickup Folder</h3>
        <div class="value">{30}</div>
    </div>
    """, unsafe_allow_html=True)

# Footer
st.markdown('<div class="footer">© 2024 DataSyncX | Version 1.0 | Contact: admin@datasyncx.com</div>', unsafe_allow_html=True)
