import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from pymongo import MongoClient
import plotly.express as px
import configparser
import os
from datetime import datetime, timedelta
import plotly.graph_objects as go
import traceback
from io import BytesIO

# Set page config must be the first Streamlit command
st.set_page_config(
    page_title="DataSyncX Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def check_password():
    """Returns `True` if the user had the correct credentials."""
    
    # Custom CSS for login page only
    st.markdown("""
        <style>
            .login-container {
                max-width: 400px;
                margin: 0 auto;
                padding: 2rem;
                background: white;
                border-radius: 10px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            .login-header {
                text-align: center;
                margin-bottom: 2rem;
            }
            .login-logo {
                font-size: 3rem;
                margin-bottom: 1rem;
            }
            .stButton > button {
                width: 100%;
            }
        </style>
    """, unsafe_allow_html=True)
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        def credentials_entered():
            """Checks whether credentials entered by the user are correct."""
            # Get username and password from secrets
            valid_username1 = st.secrets["credentials"]["username1"]
            valid_username2 = st.secrets["credentials"]["username2"]
            valid_password = st.secrets["credentials"]["password"]
            
            # Check if entered credentials are correct
            if (st.session_state["username"] in [valid_username1, valid_username2] and 
                st.session_state["password"] == valid_password):
                st.session_state["password_correct"] = True
            else:
                st.session_state["password_correct"] = False

        # First run, show input for credentials
        if "password_correct" not in st.session_state:
            # Show logo and company name only on initial password screen
            st.markdown("""
                <div class="login-container">
                    <div class="login-header">
                        <div class="login-logo">🔄</div>
                        <h1 style='color: #4CAF50; margin-bottom: 0.5rem;'>DataSyncX</h1>
                        <p style='color: #666; font-size: 0.9rem;'>Enterprise Data Synchronization Solution</p>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
            st.text_input(
                "Username", 
                key="username",
                placeholder="Enter your username"
            )
            st.text_input(
                "Password", 
                type="password", 
                on_change=credentials_entered, 
                key="password",
                placeholder="••••••••"
            )
            st.markdown("""
                <p style='text-align: center; color: #666; font-size: 0.8rem; margin-top: 1rem;'>
                    Contact your administrator if you need access.
                </p>
            """, unsafe_allow_html=True)
            return False
        
        # Credentials incorrect
        elif not st.session_state["password_correct"]:
            st.text_input(
                "Username", 
                key="username",
                placeholder="Enter your username"
            )
            st.text_input(
                "Password", 
                type="password", 
                on_change=credentials_entered, 
                key="password",
                placeholder="••••••••"
            )
            st.error("😕 Incorrect username or password")
            st.markdown("""
                <p style='text-align: center; color: #666; font-size: 0.8rem; margin-top: 1rem;'>
                    Forgot your password? Contact your administrator.
                </p>
            """, unsafe_allow_html=True)
            return False
        
        return True

# Check password before showing the main app
if not check_password():
    st.stop()

# Main app CSS (only shown after successful login)
st.markdown("""
    <style>
        .main {
            padding: 2rem;
        }
        .stMetric {
            background: linear-gradient(to right, #4CAF50, #45a049);
            padding: 1rem;
            border-radius: 10px;
            color: white;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .section-header {
            padding: 1rem;
            background: #f8f9fa;
            border-radius: 10px;
            margin: 1rem 0;
        }
        .dataframe {
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        .sidebar .sidebar-content {
            background: #2c3e50;
        }
        /* Button styling */
        .stButton>button {
            width: auto;
            padding: 0.5rem 1rem;
            background-color: #4CAF50;
            color: white;
            border-radius: 5px;
            border: none;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        .stButton>button:hover {
            background-color: #45a049;
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        }
    </style>
""", unsafe_allow_html=True)

# Load configuration
@st.cache_resource
def load_config():
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'props.properties')
        if not os.path.exists(config_path):
            st.error("⚠️ Configuration file not found!")
            st.stop()
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    except Exception as e:
        st.error(f"⚠️ Error loading configuration: {e}")
        st.stop()


# Initialize MongoDB
@st.cache_resource
def init_mongodb(_config):
    try:
        client = MongoClient(_config['DEFAULT']['mongodb_uri'])
        client.admin.command('ping')
        return client
    except Exception as e:
        st.error(f"⚠️ MongoDB Connection Failed: {e}")
        return None


# Load config and initialize MongoDB
config = load_config()
client = init_mongodb(config)

if not client:
    st.error("⚠️ Database connection failed!")
    st.stop()

# Initialize databases
db = client[config['DEFAULT']['mongodb_database']]
log_db = client[config['DEFAULT']['mongodb_log_database']]


# Add this helper function at the top level
def normalize_link_field(df):
    """Normalize the LINK field in the dataframe"""
    try:
        # Convert LINK field to string representation if it exists
        if 'LINK' in df.columns:
            df['LINK'] = df['LINK'].apply(lambda x: str(x) if x is not None else '')
        return df
    except Exception as e:
        st.error(f"Error normalizing LINK field: {e}")
        return df


# Sidebar Navigation
with st.sidebar:
    # Logo/Header Section
    st.markdown("""
        <div style='text-align: center; padding: 1rem;'>
            <h2 style='color: #4CAF50;'>
                <span style='font-size: 2rem;'>🔄</span><br>
                DataSyncX
            </h2>
            <p style='color: #666; font-size: 0.8rem;'>Monitoring Dashboard</p>
        </div>
    """, unsafe_allow_html=True)

    # Navigation Menu
    selected = option_menu(
        menu_title=None,
        options=["Dashboard", "Processing History", "Activity Info", "DHR Documents", "S3 Logs","Email Notifications", "Settings", "About"],
        icons=['speedometer2', 'clock-history', 'activity', 'folder', 'cloud-upload', 'envelope', 'gear', 'info-circle'],
        menu_icon="cast",
        default_index=0,
        styles={
            "container": {"padding": "0!important", "background-color": "#fafafa"},
            "icon": {"color": "#4CAF50", "font-size": "1rem"},
            "nav-link": {"font-size": "0.9rem", "text-align": "left", "margin": "0px"},
            "nav-link-selected": {"background-color": "#4CAF50"},
        }
    )

    # Additional Sidebar Info
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
        <div style='padding: 1rem; background: #f1f1f1; border-radius: 10px;'>
            <h4 style='color: #333;'>System Status</h4>
            <p style='color: #666; font-size: 0.8rem;'>
                🟢 Database Connected<br>
                📁 File System Active<br>
                ☁️ S3 Storage Ready
            </p>
        </div>
    """, unsafe_allow_html=True)

    # Version Info
    st.sidebar.markdown("""
        <div style='position: fixed; bottom: 0; padding: 1rem;'>
            <p style='color: #666; font-size: 0.7rem;'>
                DataSyncX Monitor v1.0<br>
                © 2024 All rights reserved
            </p>
        </div>
    """, unsafe_allow_html=True)

# Main Content
if selected == "Dashboard":
    # Header
    st.title("📊 DataSyncX Dashboard")

    # Metrics Row
    st.subheader("📈 Key Metrics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        try:
            total_docs = db[config['DEFAULT']['mongodb_collection']].count_documents({})
            st.metric("📑 Total Processed Documents", f"{total_docs:,}")
        except Exception as e:
            st.metric("📑 Total Documents", "Error")

    with col2:
        try:
            activity_info = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].find()))
            if not activity_info.empty:
                success_rate = (activity_info['passed_files'].sum() / activity_info['total_files'].sum() * 100)
                st.metric("✅ Success Rate", f"{success_rate:.1f}%")
        except Exception as e:
            st.metric("✅ Success Rate", "Error")

    with col3:
        try:
            pickup_folder = config['DEFAULT']['pickup_folder']
            if os.path.exists(pickup_folder):
                files = [f for f in os.listdir(pickup_folder) if os.path.isfile(os.path.join(pickup_folder, f))]
                st.metric("📁 Files in Pickup", len(files))
        except Exception as e:
            st.metric("📁 Files in Pickup", "Error")

    # Charts Section
    st.subheader("📊 Processing Statistics")

    try:
        if not activity_info.empty:
            tab1, tab2 = st.tabs(["📈 Processing Trends", "📊 Size Analysis"])

            with tab1:
                fig1 = px.line(activity_info,
                               x='activity_id',
                               y='total_files',
                               title='Files Processed Over Time',
                               markers=True)
                fig1.update_layout(
                    template='plotly_white',
                    xaxis_title="Activity ID",
                    yaxis_title="Total Files"
                )
                st.plotly_chart(fig1, use_container_width=True)

            with tab2:
                fig2 = px.bar(activity_info,
                              x='activity_id',
                              y=['total_xml_size', 'total_pdf_size'],
                              title='File Size Distribution',
                              barmode='group')
                fig2.update_layout(
                    template='plotly_white',
                    xaxis_title="Activity ID",
                    yaxis_title="Size (bytes)"
                )
                st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.error(f"Error creating charts: {e}")

    # Dashboard Visualizations
    st.markdown("### 📊 Processing Analytics")
    col1, col2 = st.columns(2)

    with col1:
        # Processing Timeline using activity_start_time
        try:
            # Convert activity_start_time to datetime if it's not already
            activity_info['activity_start_time'] = pd.to_datetime(activity_info['activity_start_time'])

            # Create timeline chart
            fig1 = px.line(activity_info,
                           x='activity_start_time',
                           y='total_files',
                           title='Processing Volume Over Time',
                           labels={
                               'activity_start_time': 'Processing Time',
                               'total_files': 'Number of Files'
                           })

            fig1.update_layout(
                xaxis_title="Processing Time",
                yaxis_title="Number of Files",
                hovermode='x unified'
            )
            st.plotly_chart(fig1, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating timeline chart: {str(e)}")

    with col2:
        # Success/Failure Ratio
        try:
            success_data = pd.DataFrame({
                'Status': ['Passed', 'Failed'],
                'Count': [
                    activity_info['passed_files'].sum(),
                    activity_info['failed_files'].sum()
                ]
            })

            fig2 = px.pie(success_data,
                          values='Count',
                          names='Status',
                          title='Processing Success Rate',
                          color='Status',
                          color_discrete_map={
                              'Passed': '#4CAF50',
                              'Failed': '#f44336'
                          })

            fig2.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig2, use_container_width=True)
        except Exception as e:
            st.error(f"Error creating pie chart: {str(e)}")

    try:
        # Get data from your MongoDB collection
        original_data = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_history_collection']].find()))

        # Display filters
        with st.expander("🔍 Filters", expanded=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                # Date Range Filter
                date_range = st.date_input(
                    "📅 Select Date Range",
                    value=(datetime.now() - timedelta(days=7), datetime.now()),
                    key="history_date_filter"
                )

            with col2:
                # Activity Filter
                try:
                    activity_ids = list(
                        log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].distinct('activity_id'))
                    if activity_ids:
                        selected_activity = st.selectbox(
                            "🔖 Select Activity ID",
                            activity_ids,
                            key="history_activity_filter"
                        )
                    else:
                        st.info("No activities found")
                        selected_activity = None
                except Exception as e:
                    st.error(f"Error loading activities: {e}")
                    selected_activity = None

            with col3:
                # Status Filter - Updated to use status_id
                try:
                    # Get unique status_ids from the history collection
                    status_ids = ['All'] + sorted(
                        list(log_db[config['DEFAULT']['mongodb_log_history_collection']].distinct('status')))
                    selected_status = st.selectbox(
                        "⚡ Status",
                        status_ids,
                        key="history_status_filter"
                    )
                except Exception as e:
                    st.error(f"Error loading status IDs: {e}")
                    selected_status = 'All'

        # Apply filters to the data
        filtered_data = original_data.copy()

        # Apply date filter
        if 'timestamp' in filtered_data.columns:
            filtered_data['timestamp'] = pd.to_datetime(filtered_data['timestamp'])
            mask = (filtered_data['timestamp'].dt.date >= date_range[0]) & \
                   (filtered_data['timestamp'].dt.date <= date_range[1])
            filtered_data = filtered_data[mask]

        # Apply activity filter
        if selected_activity:
            filtered_data = filtered_data[filtered_data['activity_id'] == selected_activity]

        # Apply status filter - Updated to use status_id
        if selected_status != 'All':
            filtered_data = filtered_data[filtered_data['status'] == selected_status]

        # Display summary metrics
        st.markdown("### 📊 Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(filtered_data))
        with col2:
            st.metric("Date Range", f"{date_range[0]} to {date_range[1]}")
        with col3:
            st.metric("Selected Activity", selected_activity if selected_activity else "All")
        with col4:
            st.metric("Status ID", selected_status)

        # Display the filtered data
        st.markdown("### 📋 Filtered Records")
        st.dataframe(
            filtered_data,
            use_container_width=True,
            height=400
        )

        # Export Option
        if st.button("📥 Export Filtered Data"):
            csv = filtered_data.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Error processing data: {str(e)}")
        with st.expander("View Error Details"):
            st.code(traceback.format_exc())
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()



elif selected == "Processing History":
    st.markdown('<h1 style="color: #333;">📜 Processing History</h1>', unsafe_allow_html=True)

    try:
        # Fetch the history data
        pair_history = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_pair_history_collection']].find()))

        if not pair_history.empty:
            # Debug info
            st.write("Available columns:", pair_history.columns.tolist())

            # Add filters section
            st.markdown(
                '<div style="background-color: #f8f9fa; padding: 1rem; border-radius: 10px; margin-bottom: 1rem;">',
                unsafe_allow_html=True)

            col1, col2, col3 = st.columns(3)

            with col1:
                # Date filter
                st.markdown('<p style="color: #333; font-weight: 500;">📅 Date Range</p>', unsafe_allow_html=True)
                date_filter = st.date_input(
                    "",
                    value=(datetime.now() - timedelta(days=7), datetime.now())
                )

            with col2:
                # Activity ID filter
                if 'activity_id' in pair_history.columns:
                    st.markdown('<p style="color: #333; font-weight: 500;">🔖 Activity ID</p>', unsafe_allow_html=True)
                    activity_ids = ['All'] + sorted(pair_history['activity_id'].unique().tolist())
                    selected_activity = st.selectbox("", activity_ids)
                else:
                    st.warning("Activity ID field not found")
                    selected_activity = 'All'

            with col3:
                # DHR ID filter
                if 'dhr_id' in pair_history.columns:
                    st.markdown('<p style="color: #333; font-weight: 500;">🔍 DHR ID</p>', unsafe_allow_html=True)
                    dhr_ids = ['All'] + sorted(pair_history['dhr_id'].unique().tolist())
                    selected_dhr = st.selectbox("", dhr_ids)
                else:
                    st.warning("DHR ID field not found")
                    selected_dhr = 'All'

            st.markdown('</div>', unsafe_allow_html=True)

            # Apply filters
            filtered_history = pair_history.copy()

            # Convert timestamp to datetime if it exists
            if 'timestamp' in filtered_history.columns:
                filtered_history['timestamp'] = pd.to_datetime(filtered_history['timestamp'])

                # Apply date filter
                mask = (filtered_history['timestamp'].dt.date >= date_filter[0]) & \
                       (filtered_history['timestamp'].dt.date <= date_filter[1])
                filtered_history = filtered_history[mask]

            # Apply other filters
            if selected_activity != 'All' and 'activity_id' in filtered_history.columns:
                filtered_history = filtered_history[filtered_history['activity_id'] == selected_activity]

            if selected_dhr != 'All' and 'dhr_id' in filtered_history.columns:
                filtered_history = filtered_history[filtered_history['dhr_id'] == selected_dhr]

            # Display summary metrics
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)
            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

            with metric_col1:
                total_records = len(filtered_history)
                st.metric("Total Records", f"{total_records:,}")

            with metric_col2:
                if 'dhr_id' in filtered_history.columns:
                    unique_dhrs = filtered_history['dhr_id'].nunique()
                    st.metric("Unique DHRs", f"{unique_dhrs:,}")
                else:
                    st.metric("Unique DHRs", "N/A")

            # Display the data
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📋 Records</h3>', unsafe_allow_html=True)

            # Format timestamp if it exists
            display_history = filtered_history.copy()
            if 'timestamp' in display_history.columns:
                display_history['timestamp'] = display_history['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Display the dataframe
            st.dataframe(
                display_history,
                use_container_width=True,
                height=400,
            )

            # Export functionality
            st.markdown("---")
            if st.button("📥 Export to CSV"):
                csv = display_history.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"processing_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        else:
            st.info("No processing history available")

    except Exception as e:
        st.error(f"Error loading history: {str(e)}")
        with st.expander("View Error Details"):
            st.code(f"""
Error Type: {type(e).__name__}
Error Message: {str(e)}
Stack Trace:
{traceback.format_exc()}
            """)
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()

elif selected == "Activity Info":
    st.markdown('<h1 style="color: #333;">📊 Activity Information</h1>', unsafe_allow_html=True)

    try:
        activity_info = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_activity_info_collection']].find()))

        if not activity_info.empty:
            # Debug: Show available columns
            st.write("Available columns:", activity_info.columns.tolist())

            # Filters Section
            with st.expander("🔍 Filters", expanded=True):
                col1, col2 = st.columns(2)

                with col1:
                    # Date Range Filter
                    st.markdown('<p style="color: #333; font-weight: 500;">📅 Date Range</p>', unsafe_allow_html=True)
                    date_filter = st.date_input(
                        "",
                        value=(datetime.now() - timedelta(days=7), datetime.now())
                    )

                with col2:
                    # Activity ID Filter
                    if 'activity_id' in activity_info.columns:
                        st.markdown('<p style="color: #333; font-weight: 500;">🔖 Activity ID</p>',
                                    unsafe_allow_html=True)
                        activity_ids = ['All'] + sorted(activity_info['activity_id'].unique().tolist())
                        selected_activity = st.selectbox("", activity_ids)
                    else:
                        selected_activity = 'All'

            # Apply filters
            filtered_activity_info = activity_info.copy()

            # Convert timestamp if it exists
            if 'timestamp' in filtered_activity_info.columns:
                filtered_activity_info['timestamp'] = pd.to_datetime(filtered_activity_info['timestamp'])

                # Apply date filter
                mask = (filtered_activity_info['timestamp'].dt.date >= date_filter[0]) & \
                       (filtered_activity_info['timestamp'].dt.date <= date_filter[1])
                filtered_activity_info = filtered_activity_info[mask]

            # Apply activity filter
            if selected_activity != 'All' and 'activity_id' in filtered_activity_info.columns:
                filtered_activity_info = filtered_activity_info[
                    filtered_activity_info['activity_id'] == selected_activity]

            # Display summary metrics
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)

            metric_col1, metric_col2, metric_col3 = st.columns(3)

            with metric_col1:
                total_records = len(filtered_activity_info)
                st.metric("Total Records", f"{total_records:,}")

            with metric_col2:
                if 'activity_id' in filtered_activity_info.columns:
                    unique_activities = filtered_activity_info['activity_id'].nunique()
                    st.metric("Unique Activities", f"{unique_activities:,}")

            with metric_col3:
                if 'file_size' in filtered_activity_info.columns:
                    total_size = filtered_activity_info['file_size'].sum() / (1024 * 1024)  # Convert to MB
                    st.metric("Total Size", f"{total_size:.2f} MB")

            # Display the data
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📋 Records</h3>', unsafe_allow_html=True)

            # Format timestamp for display
            display_activity_info = filtered_activity_info.copy()
            if 'timestamp' in display_activity_info.columns:
                display_activity_info['timestamp'] = display_activity_info['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Display as a table
            st.dataframe(
                display_activity_info,
                use_container_width=True,
                height=400
            )

            # Export functionality
            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                if st.button("📥 Export to CSV"):
                    csv = display_activity_info.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"activity_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )



        else:
            st.info("No activity information available")

    except Exception as e:
        st.error(f"Error loading activity information: {str(e)}")
        with st.expander("View Error Details"):
            st.code(f"""
Error Type: {type(e).__name__}
Error Message: {str(e)}
Available Columns: {activity_info.columns.tolist() if 'activity_info' in locals() and not activity_info.empty else 'No data'}
Stack Trace:
{traceback.format_exc()}
            """)
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()

elif selected == "DHR Documents":
    st.markdown('<h1 style="color: #333;">📁 DHR Documents</h1>', unsafe_allow_html=True)

    try:
        dhr_documents = pd.DataFrame(list(db[config['DEFAULT']['mongodb_collection']].find()))

        if not dhr_documents.empty:
            dhr_documents = normalize_link_field(dhr_documents)


            # Extract download URL and create direct download link
            def create_download_link(link_str):
                try:
                    if isinstance(link_str, str) and 'cloudinary.com' in link_str:
                        # Extract URL using string manipulation
                        start = link_str.find("'DOWNLOAD_URL': '") + len("'DOWNLOAD_URL': '")
                        end = link_str.find("'", start)
                        url = link_str[start:end]
                        # Modify URL to force download
                        if url:
                            # Add Cloudinary parameters to force download
                            url = url.replace('/upload/', '/upload/fl_attachment/')
                            # Add Content-Disposition header
                            filename = url.split('/')[-1]
                            url = f"{url}?attachment={filename}"
                        return url
                    return None
                except:
                    return None


            # Add download URL column
            if 'LINK' in dhr_documents.columns:
                dhr_documents['download_url'] = dhr_documents['LINK'].apply(create_download_link)

            # Filters Section
            with st.expander("🔍 Filters", expanded=True):
                col1, col2, col3 = st.columns(3)

                with col1:
                    # Date Range Filter
                    st.markdown('<p style="color: #333; font-weight: 500;">📅 Date Range</p>', unsafe_allow_html=True)
                    date_filter = st.date_input(
                        "",
                        value=(datetime.now() - timedelta(days=7), datetime.now())
                    )

                with col2:
                    # Activity ID Filter
                    if 'ACTIVITY_ID' in dhr_documents.columns:
                        st.markdown('<p style="color: #333; font-weight: 500;">🔖 Activity ID</p>',
                                    unsafe_allow_html=True)
                        activity_ids = ['All'] + sorted(list(dhr_documents['ACTIVITY_ID'].unique()))
                        selected_activity = st.selectbox(
                            "",  # Empty label since we use markdown above
                            activity_ids,
                            key="dhr_activity_filter"
                        )
                    else:
                        st.warning("Activity ID field not found")
                        selected_activity = 'All'

                with col3:
                    # OFFSET Filter
                    if 'OFFSET' in dhr_documents.columns:
                        st.markdown('<p style="color: #333; font-weight: 500;">🔍 OFFSET</p>', unsafe_allow_html=True)
                        offset_values = ['All'] + sorted(list(dhr_documents['OFFSET'].unique()))
                        selected_offset = st.selectbox(
                            "",  # Empty label since we use markdown above
                            offset_values,
                            key="dhr_offset_filter"
                        )
                    else:
                        st.warning("OFFSET field not found")
                        selected_offset = 'All'

            # Apply filters
            filtered_dhr_documents = dhr_documents.copy()

            # Apply date filter
            if 'timestamp' in filtered_dhr_documents.columns:
                filtered_dhr_documents['timestamp'] = pd.to_datetime(filtered_dhr_documents['timestamp'])
                mask = (filtered_dhr_documents['timestamp'].dt.date >= date_filter[0]) & \
                       (filtered_dhr_documents['timestamp'].dt.date <= date_filter[1])
                filtered_dhr_documents = filtered_dhr_documents[mask]

            # Apply activity filter
            if selected_activity != 'All' and 'ACTIVITY_ID' in filtered_dhr_documents.columns:
                filtered_dhr_documents = filtered_dhr_documents[
                    filtered_dhr_documents['ACTIVITY_ID'] == selected_activity]

            # Apply OFFSET filter
            if selected_offset != 'All' and 'OFFSET' in filtered_dhr_documents.columns:
                filtered_dhr_documents = filtered_dhr_documents[filtered_dhr_documents['OFFSET'] == selected_offset]

            # Update metrics display
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)
            metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

            with metric_col1:
                total_records = len(filtered_dhr_documents)
                st.metric("Total Records", f"{total_records:,}")

            with metric_col2:
                if 'ACTIVITY_ID' in filtered_dhr_documents.columns:
                    unique_activities = filtered_dhr_documents['ACTIVITY_ID'].nunique()
                    st.metric("Unique Activities", f"{unique_activities:,}")

            with metric_col3:
                if 'OFFSET' in filtered_dhr_documents.columns:
                    unique_offsets = filtered_dhr_documents['OFFSET'].nunique()
                    st.metric("Unique OFFSETs", f"{unique_offsets:,}")

            with metric_col4:
                st.metric("Date Range", f"{date_filter[0]} to {date_filter[1]}")

            # Display the data
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📋 Records</h3>', unsafe_allow_html=True)

            # Format timestamp for display
            display_dhr_documents = filtered_dhr_documents.copy()
            if 'timestamp' in display_dhr_documents.columns:
                display_dhr_documents['timestamp'] = display_dhr_documents['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Move download_url to the first column if it exists
            if 'download_url' in display_dhr_documents.columns:
                cols = ['download_url'] + [col for col in display_dhr_documents.columns if col != 'download_url']
                display_dhr_documents = display_dhr_documents[cols]

            # Display as interactive table with download links
            st.dataframe(
                display_dhr_documents,
                use_container_width=True,
                height=400,
                column_config={
                    "download_url": st.column_config.LinkColumn(
                        "Download PDF",
                        help="Click to download PDF directly",
                        validate="^https://.*",
                        max_chars=100,
                        display_text="⬇️ Download"
                    ),
                    "timestamp": st.column_config.DatetimeColumn(
                        "Timestamp",
                        format="DD/MM/YYYY HH:mm"
                    ),
                    "OFFSET": "DHR ID",
                    "ACTIVITY_ID": "Activity ID",
                    "LINK": st.column_config.TextColumn(
                        "LINK",
                        width="medium",
                        help="Raw LINK data"
                    )
                }
            )

            # Export functionality
            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                if st.button("📥 Export to CSV"):
                    csv = display_dhr_documents.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"dhr_documents_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )

            with col2:
                st.info("💡 Click the Download links or buttons to save PDFs directly")

        else:
            st.info("No DHR documents available")

    except Exception as e:
        st.error(f"Error loading DHR documents: {str(e)}")
        with st.expander("View Error Details"):
            st.code(f"""
Error Type: {type(e).__name__}
Error Message: {str(e)}
Available Columns: {dhr_documents.columns.tolist() if 'dhr_documents' in locals() and not dhr_documents.empty else 'No data'}
Stack Trace:
{traceback.format_exc()}
            """)
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()


elif selected == "S3 Logs":
    st.markdown('<h1 style="color: #333;">☁️ S3 Storage Logs</h1>', unsafe_allow_html=True)

    try:
        s3_logs = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_s3_collection']].find()))

        if not s3_logs.empty:
            # Debug: Show available columns
            st.write("Available columns:", s3_logs.columns.tolist())

            # Filters Section
            with st.expander("🔍 Filters", expanded=True):
                col1, col2 = st.columns(2)

                with col1:
                    # Date Range Filter
                    st.markdown('<p style="color: #333; font-weight: 500;">📅 Date Range</p>', unsafe_allow_html=True)
                    date_filter = st.date_input(
                        "",
                        value=(datetime.now() - timedelta(days=7), datetime.now())
                    )

                with col2:
                    # Activity ID Filter
                    if 'activity_id' in s3_logs.columns:
                        st.markdown('<p style="color: #333; font-weight: 500;">🔖 Activity ID</p>',
                                    unsafe_allow_html=True)
                        activity_ids = ['All'] + sorted(s3_logs['activity_id'].unique().tolist())
                        selected_activity = st.selectbox("", activity_ids)
                    else:
                        selected_activity = 'All'

            # Apply filters
            filtered_logs = s3_logs.copy()

            # Convert timestamp if it exists
            if 'timestamp' in filtered_logs.columns:
                filtered_logs['timestamp'] = pd.to_datetime(filtered_logs['timestamp'])

                # Apply date filter
                mask = (filtered_logs['timestamp'].dt.date >= date_filter[0]) & \
                       (filtered_logs['timestamp'].dt.date <= date_filter[1])
                filtered_logs = filtered_logs[mask]

            # Apply activity filter
            if selected_activity != 'All' and 'activity_id' in filtered_logs.columns:
                filtered_logs = filtered_logs[filtered_logs['activity_id'] == selected_activity]

            # Display summary metrics
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)

            metric_col1, metric_col2, metric_col3 = st.columns(3)

            with metric_col1:
                total_records = len(filtered_logs)
                st.metric("Total Records", f"{total_records:,}")

            with metric_col2:
                if 'activity_id' in filtered_logs.columns:
                    unique_activities = filtered_logs['activity_id'].nunique()
                    st.metric("Unique Activities", f"{unique_activities:,}")

            with metric_col3:
                if 'file_size' in filtered_logs.columns:
                    total_size = filtered_logs['file_size'].sum() / (1024 * 1024)  # Convert to MB
                    st.metric("Total Size", f"{total_size:.2f} MB")

            # Display the data
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📋 Records</h3>', unsafe_allow_html=True)

            # Format timestamp for display
            display_logs = filtered_logs.copy()
            if 'timestamp' in display_logs.columns:
                display_logs['timestamp'] = display_logs['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Display as a table
            st.dataframe(
                display_logs,
                use_container_width=True,
                height=400
            )

            # Export functionality
            st.markdown("---")
            col1, col2 = st.columns(2)

            with col1:
                if st.button("📥 Export to CSV"):
                    csv = display_logs.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name=f"s3_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )



        else:
            st.info("No S3 logs available")

    except Exception as e:
        st.error(f"Error loading S3 logs: {str(e)}")
        with st.expander("View Error Details"):
            st.code(f"""
Error Type: {type(e).__name__}
Error Message: {str(e)}
Available Columns: {s3_logs.columns.tolist() if 's3_logs' in locals() and not s3_logs.empty else 'No data'}
Stack Trace:
{traceback.format_exc()}
            """)
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()
            
elif selected == "Email Notifications":
    st.markdown('<h1 style="color: #333;">📧 Email Notifications</h1>', unsafe_allow_html=True)

    try:
        # Fetch email logs
        email_logs = pd.DataFrame(list(log_db[config['DEFAULT']['mongodb_log_email_collection']].find()))

        if not email_logs.empty:
            # Filters Section
            with st.expander("🔍 Filters", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    # Date Range Filter
                    st.markdown('<p style="color: #333; font-weight: 500;">📅 Date Range</p>', unsafe_allow_html=True)
                    date_filter = st.date_input(
                        "",
                        value=(datetime.now() - timedelta(days=7), datetime.now())
                    )

                with col2:
                    # Activity ID Filter
                    if 'activity_id' in email_logs.columns:
                        st.markdown('<p style="color: #333; font-weight: 500;">🔖 Activity ID</p>', unsafe_allow_html=True)
                        activity_ids = ['All'] + sorted(email_logs['activity_id'].unique().tolist())
                        selected_activity = st.selectbox("", activity_ids)
                    else:
                        selected_activity = 'All'

            # Apply filters
            filtered_logs = email_logs.copy()

            # Convert timestamp if it exists
            if 'sent_timestamp' in filtered_logs.columns:
                filtered_logs['sent_timestamp'] = pd.to_datetime(filtered_logs['sent_timestamp'])
                mask = (filtered_logs['sent_timestamp'].dt.date >= date_filter[0]) & \
                       (filtered_logs['sent_timestamp'].dt.date <= date_filter[1])
                filtered_logs = filtered_logs[mask]

            # Apply activity filter
            if selected_activity != 'All' and 'activity_id' in filtered_logs.columns:
                filtered_logs = filtered_logs[filtered_logs['activity_id'] == selected_activity]

            # Display summary metrics
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📊 Summary</h3>', unsafe_allow_html=True)
            metric_col1, metric_col2, metric_col3 = st.columns(3)

            with metric_col1:
                total_emails = len(filtered_logs)
                st.metric("Total Emails", f"{total_emails:,}")

            with metric_col2:
                if 'activity_id' in filtered_logs.columns:
                    unique_activities = filtered_logs['activity_id'].nunique()
                    st.metric("Activities", f"{unique_activities:,}")

            with metric_col3:
                st.metric("Date Range", f"{date_filter[0]} to {date_filter[1]}")

            # Display the email logs
            st.markdown('<h3 style="color: #333; margin-top: 1rem;">📧 Email Logs</h3>', unsafe_allow_html=True)

            # Format timestamp for display
            display_logs = filtered_logs.copy()
            if 'sent_timestamp' in display_logs.columns:
                display_logs['sent_timestamp'] = display_logs['sent_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Display as a table with custom column configuration
            st.dataframe(
                display_logs,
                use_container_width=True,
                height=400,
                column_config={
                    "sent_timestamp": st.column_config.DatetimeColumn(
                        "Sent Time",
                        format="DD/MM/YYYY HH:mm:ss"
                    ),
                    "activity_id": st.column_config.TextColumn(
                        "Activity ID",
                        width="medium"
                    ),
                    "email_content": st.column_config.TextColumn(
                        "Email Content",
                        width="large"
                    ),
                    "status": st.column_config.TextColumn(
                        "Status",
                        width="small"
                    )
                }
            )

            # Export functionality
            st.markdown("---")
            if st.button("📥 Export Email Logs"):
                csv = display_logs.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"email_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        else:
            st.info("No email logs available")

    except Exception as e:
        st.error(f"Error loading email logs: {str(e)}")
        with st.expander("View Error Details"):
            st.code(traceback.format_exc())
    ## Refresh Button (Fixed)
    if st.button("🔄 Refresh Data"):
        st.cache_resource.clear()
        st.rerun()

elif selected == "Settings":
    st.title("⚙️ Settings")
    
    # System Settings
    st.markdown("### 🖥️ System Configuration")
    with st.expander("System Settings", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.number_input("Processing Batch Size", min_value=1, max_value=1000, value=100)
            st.number_input("Max Retries", min_value=1, max_value=10, value=3)
        with col2:
            st.selectbox("Log Level", ["DEBUG", "INFO", "WARNING", "ERROR"])
            st.text_input("Temp Directory", value="/tmp/datasyncx")

    # Database Settings
    st.markdown("### 🗄️ Database Configuration")
    with st.expander("Database Settings", expanded=True):
        st.text_input("MongoDB URI", value="mongodb://localhost:27017", type="password")
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Database Name", value="datasyncx")
            st.text_input("Collection Name", value="documents")
        with col2:
            st.text_input("Log Database", value="datasyncx_logs")
            st.text_input("Log Collection", value="activity_logs")

    # Storage Settings
    st.markdown("### 📂 Storage Configuration")
    with st.expander("Storage Settings", expanded=True):
        st.text_input("S3 Bucket Name", value="datasyncx-storage")
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("AWS Access Key", type="password")
            st.text_input("AWS Region", value="us-east-1")
        with col2:
            st.text_input("AWS Secret Key", type="password")
            st.checkbox("Enable S3 Versioning", value=True)

    # Notification Settings
    st.markdown("### 📧 Notification Settings")
    with st.expander("Email Notifications", expanded=True):
        st.text_input("SMTP Server", value="smtp.gmail.com")
        col1, col2 = st.columns(2)
        with col1:
            st.text_input("SMTP Username")
            st.text_input("Sender Email")
        with col2:
            st.text_input("SMTP Password", type="password")
            st.text_input("Recipients (comma-separated)")
        st.multiselect("Notification Events", 
            ["Processing Complete", "Error Occurred", "System Warning", "Daily Summary"],
            ["Error Occurred", "Daily Summary"]
        )

    # Advanced Settings
    st.markdown("### ⚡ Advanced Settings")
    with st.expander("Advanced Configuration", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            st.checkbox("Enable Debug Mode", value=False)
            st.checkbox("Auto-retry Failed Tasks", value=True)
        with col2:
            st.checkbox("Compress Logs", value=True)
            st.checkbox("Enable Performance Metrics", value=True)
        st.slider("Log Retention Days", min_value=1, max_value=365, value=30)

    # Save Button
    if st.button("💾 Save Settings"):
        st.success("Settings saved successfully!")
        st.info("Note: Some changes may require a system restart to take effect.")


# Add metrics summary




elif selected == "About":
    # Title and Overview
    st.markdown("""
    # 🔄 DataSyncX - Enterprise Data Synchronization Solution
    
    ## Overview
    DataSyncX is a state-of-the-art Device History Record (DHR) processing and synchronization platform, designed specifically for medical device manufacturers and healthcare industries worldwide. This enterprise-grade solution streamlines the critical process of managing, validating, and archiving Device History Records, ensuring compliance with global regulatory requirements including FDA 21 CFR Part 11 and ISO 13485.

    ### 🏥 Industry Focus
    - **Medical Device Manufacturing**: Specialized in processing Device History Records (DHRs)
    - **Healthcare Compliance**: Meets international regulatory standards
    - **Global Operations**: Supports worldwide manufacturing facilities
    - **Quality Assurance**: Ensures data integrity and traceability

    ### 💫 Core Capabilities
    - Automated DHR document pairing and validation
    - Real-time processing status monitoring
    - Secure cloud storage with versioning
    - Comprehensive audit trails
    - Intelligent error detection and handling
    - Automated compliance reporting

    ### 🌟 Business Impact
    - Reduces manual DHR processing time by up to 80%
    - Minimizes human errors in document processing
    - Ensures regulatory compliance across global operations
    - Provides real-time visibility into processing status
    - Enables faster product release cycles
    """)

    # Key Features in columns
    st.markdown("### 🎯 Key Features")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        #### 📊 Real-Time Monitoring
        - Live dashboard metrics
        - Processing status tracking
        - System health monitoring
        - Resource utilization
        - Performance analytics
        - Custom alerts
        
        #### 📁 Document Management
        - Secure storage & retrieval
        - Version control
        - Audit trails
        - File organization
        - Multiple format support
        - Batch processing
        """)

    with col2:
        st.markdown("""
        #### 🔄 Automated Processing
        - Auto document pickup
        - XML/PDF handling
        - File pairing system
        - Processing rules
        - Validation checks
        - Error handling
        
        #### 📧 Notifications
        - Email alerts
        - Custom rules
        - Status updates
        - Error notifications
        - Batch summaries
        - Activity reports
        """)

    with col3:
        st.markdown("""
        #### 📈 Advanced Analytics
        - Performance metrics
        - Success/failure analysis
        - Trend visualization
        - Custom reports
        - Data insights
        - KPI tracking
        
        #### 🔒 Security
        - Encrypted storage
        - Secure transfer
        - Access control
        - Audit logging
        - Data protection
        - Compliance ready
        """)

    # Target Users in columns
    st.markdown("### 👥 Target Users")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        #### 🏭 Production Teams
        - Monitor processing status
        - Track efficiency
        - Manage workflows
        - Handle exceptions
        
        #### 👨‍💻 System Administrators
        - Configure settings
        - Monitor health
        - Manage access
        - Handle errors
        """)

    with col2:
        st.markdown("""
        #### ✅ Quality Assurance
        - Track accuracy
        - Monitor errors
        - Validate pairs
        - Ensure compliance
        
        #### 📊 Business Analysts
        - Generate reports
        - Analyze trends
        - Track KPIs
        - Optimize processes
        """)

    # Technical Specs and Benefits in columns
    st.markdown("### 🛠️ Technical Specifications & Benefits")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        #### System Requirements
        - Windows Server 2016+
        - Python 3.8+
        - MongoDB 4.4+
        - 8GB RAM
        - 100GB storage
        """)

    with col2:
        st.markdown("""
        #### Integration
        - REST API support
        - S3 storage
        - SMTP email
        - Custom webhooks
        - Database connectors
        """)

    with col3:
        st.markdown("""
        #### Key Benefits
        - Reduced processing time
        - Improved accuracy
        - Real-time monitoring
        - Cost efficiency
        - Scalable solution
        """)

    # Support Information
    st.markdown("### 📞 Support & Resources")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        #### Documentation
        - User guides
        - API documentation
        - Configuration guides
        - Troubleshooting
        
        #### Training
        - Online documentation
        - Video tutorials
        - Custom sessions
        - Best practices
        """)

    with col2:
        st.markdown("""
        #### Technical Support
        - Email: support@datasyncx.com
        - Response time: 24-48 hours
        - Priority support available
        - Expert assistance
        
        #### Version Info
        - Version: 1.0.0
        - Released: 2024
        - Last Updated: "2024-11-09"
        """)

    # Contact Form
    st.markdown("### 📬 Contact Us")
    with st.form("contact_form"):
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Name")
            email = st.text_input("Email")
        with col2:
            subject = st.text_input("Subject")
            message = st.text_area("Message")
        
        submitted = st.form_submit_button("Send Message")
        if submitted:
            st.success("Thank you for your message! We'll get back to you soon.")

    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: #666; padding: 20px;'>
            DataSyncX © 2024 All rights reserved
        </div>
    """, unsafe_allow_html=True)

