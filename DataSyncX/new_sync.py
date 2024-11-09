import logging
import os
import shutil
import json
import csv
import sys

import boto3
from pymongo import MongoClient
from lxml import etree
from datetime import datetime
import configparser
import xmltodict
import traceback
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import cloudinary
import cloudinary.uploader
import cloudinary.api
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException


def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = getattr(sys, '_MEIPASS', os.path.abspath('.'))
    except AttributeError:
        base_path = os.path.abspath('.')
    return os.path.join(base_path, relative_path)

# Define paths for the resources
properties_path = resource_path('props.properties')
xsd_path = resource_path('document.xsd')

def get_props_path():
    """Get the external properties file path"""
    if getattr(sys, 'frozen', False):
        # If running as executable, use the executable's directory
        exe_dir = os.path.dirname(sys.executable)
        return os.path.join(exe_dir, 'props.properties')
    else:
        # If running as script, use the script's directory
        return os.path.join(os.path.dirname(__file__), 'props.properties')

def ensure_props_file():
    """Ensure properties file exists in the correct location"""
    props_path = get_props_path()
    if not os.path.exists(props_path):
        # Copy from bundled resources to external location if needed
        bundled_props = resource_path('props.properties')
        shutil.copy2(bundled_props, props_path)
    return props_path

def read_properties(file_path):
    config = configparser.ConfigParser()
    with open(file_path, 'r') as f:
        config.read_file(f)
    return config

def update_properties(config, file_path):
    """Update properties file with new values"""
    with open(file_path, 'w') as configfile:
        config.write(configfile)

# Initialize configuration
try:
    print("Starting application...")
    print(f"Executable path: {sys.executable}")
    print(f"Current working directory: {os.getcwd()}")
    
    # Get external properties file path
    properties_path = ensure_props_file()
    xsd_path = resource_path('document.xsd')
    
    print(f"Using properties file at: {properties_path}")
    config = read_properties(properties_path)
    print("Properties loaded successfully")
    
except Exception as e:
    print(f"Critical error during initialization: {e}")
    traceback.print_exc()
    sys.exit(1)

# Setup the main logger
def setup_logger(activity_id):
    log_dir = "C:/DataSyncX/logs/"
    log_filename = f"{log_dir}\\activity_{activity_id}.log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logger = logging.getLogger(activity_id)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

# Update the get_next_id function to use the external properties file
def get_next_id(counter_name, logger):
    try:
        current_id = int(config['DEFAULT'][counter_name])
        next_id = current_id + 1
        config['DEFAULT'][counter_name] = str(next_id).zfill(16) if counter_name == "arc_doc_id" else str(next_id)
        update_properties(config, properties_path)  # Use the external properties file
        return config['DEFAULT'][counter_name]
    except Exception as e:
        logger.error(f"Failed to update {counter_name}. Error: {e}")
        return None


# History logging
def log_pair_history(activity_id, pair_name, status, timestamp,dhr_id, logger):

    status_id = get_next_id('status_id', logger)
    log_entry = {
        "status_id": status_id,
        'dhr_id': dhr_id,
        "activity_id": activity_id,
        "pair_name": pair_name,
        "status": status,
        "timestamp": timestamp,
    }
    insert_into_mongodb(log_entry, config['DEFAULT']['mongodb_uri'], config['DEFAULT']['mongodb_log_database'], config['DEFAULT']['mongodb_log_History_collection'], logger)

# Activity info logging
def log_activity_info(activity_id, total_files, passed_files, failed_files, total_xml_size, total_pdf_size, activity_start_time, activity_end_time,logger):
    log_entry = {
        "activity_id": activity_id,
        "total_files": total_files,
        "passed_files": passed_files,
        "failed_files": failed_files,
        "total_xml_size": total_xml_size,
        "total_pdf_size": total_pdf_size,
        "activity_start_time": activity_start_time,
        "activity_end_time": activity_end_time
    }
    insert_into_mongodb(log_entry, config['DEFAULT']['mongodb_uri'], config['DEFAULT']['mongodb_log_database'], config['DEFAULT']['mongodb_log_activity_info_collection'], logger)

    logger.info(log_entry)

# Log events for each record
def log_events(activity_id, pair_name, log_entries,dhr_id,logger):
    event_id = get_next_id('event_id', logger)
    log_entry = {
        "events_id": event_id,
        'dhr_id': dhr_id,
        "pair_name": pair_name,
        "log": log_entries,
        "timestamp": datetime.now().isoformat(),
        "activity_id": activity_id
    }
    insert_into_mongodb(log_entry, config['DEFAULT']['mongodb_uri'], config['DEFAULT']['mongodb_log_database'], config['DEFAULT']['mongodb_log_pair_history_collection'], logger)

# S3 info logging
def log_s3_info(activity_id, pair_name, s3_key,dhr_id, logger):
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "activity_id": activity_id,
        'dhr_id': dhr_id,
        "pair_name": pair_name,
        "s3_key": s3_key
    }
    insert_into_mongodb(log_entry, config['DEFAULT']['mongodb_uri'], config['DEFAULT']['mongodb_log_database'], config['DEFAULT']['mongodb_log_s3_collection'], logger)

def validate_xml(xml_file, xsd_file, logger):
    try:
        # Read and parse the XSD schema
        with open(xsd_file, 'rb') as xsd:
            schema_root = etree.XML(xsd.read())
        schema = etree.XMLSchema(schema_root)

        # Read and parse the XML file as bytes to avoid encoding issues
        with open(xml_file, 'rb') as xml:
            xml_doc = etree.parse(xml)

        # Validate the XML file against the schema
        schema.assertValid(xml_doc)

        logger.info(f"Validation successful for XML file: {xml_file}")
        return True

    except (etree.XMLSyntaxError, etree.DocumentInvalid) as e:
        logger.error(f"Validation failed for XML file: {xml_file}. Error: {e}")
        return False
def validate_json(json_file, logger):
    try:
        with open(json_file, 'r') as file:
            json_data = json.load(file)
        logger.info(f"Validation successful for JSON file: {json_file}")
        return True
    except json.JSONDecodeError as e:
        logger.error(f"Validation failed for JSON file: {json_file}. Error: {e}")
        return False

def validate_csv(csv_file, logger):
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                pass  # Just read through the file to check for errors
        logger.info(f"Validation successful for CSV file: {csv_file}")
        return True
    except csv.Error as e:
        logger.error(f"Validation failed for CSV file: {csv_file}. Error: {e}")
        return False

def validate_files(data_file, pdf_file, logger):
    try:
        if not os.path.exists(data_file):
            raise FileNotFoundError(f"Data file {data_file} not found.")
        if not os.path.exists(pdf_file):
            raise FileNotFoundError(f"PDF file {pdf_file} not found.")

        file_ext = os.path.splitext(data_file)[1].lower()

        if file_ext == '.xml':
            if validate_xml(data_file, xsd_path, logger):
                logger.info(f"Validation successful for pair: {data_file} and {pdf_file}")
                return True
            else:
                raise Exception("XML validation failed")
        elif file_ext == '.json':
            if validate_json(data_file, logger):
                logger.info(f"Validation successful for pair: {data_file} and {pdf_file}")
                return True
            else:
                raise Exception("JSON validation failed")
        elif file_ext == '.csv':
            if validate_csv(data_file, logger):
                logger.info(f"Validation successful for pair: {data_file} and {pdf_file}")
                return True
            else:
                raise Exception("CSV validation failed")
        else:
            raise Exception(f"Unsupported file type: {file_ext}")
    except Exception as e:
        logger.error(f"Validation failed for pair: {data_file} and {pdf_file}. Error: {e}")
        return False

def extract_and_transform(data_file, pdf_file, activity_id, logger):
    try:
        base_name, ext = os.path.splitext(data_file)

        if ext == '.xml':
            with open(data_file) as f:
                xml_content = f.read()
                document_content = xmltodict.parse(xml_content).get('document', {})

        elif ext == '.json':
            with open(data_file) as f:
                raw_data = json.load(f)

            # Extract the relevant fields from the original structure
            document = raw_data.get("Documents", [])[0]  # Assuming there's at least one document
            fields = document.get("Fields", {})

            # Parse the date and reformat it to YYYYMMDD
            original_date = fields.get("Date_of_Manufacture", "")
            try:
                date_of_manufacture = datetime.strptime(original_date, "%d-%b-%Y").strftime("%Y%m%d")
            except ValueError:
                date_of_manufacture = original_date  # If parsing fails, use the original value

            # Create the new structure
            document_content = {
                "BatchID": raw_data.get("BatchID", ""),
                "DocumentID": document.get("DocumentID", ""),
                "DocumentUUID": document.get("DocumentUUID", ""),
                "Material": fields.get("Material", ""),
                "Batch": fields.get("Batch", ""),
                "Production_Order": fields.get("Production_Order", ""),
                "Material_Description": fields.get("Material_Description", ""),
                "Date_of_Manufacture": date_of_manufacture
            }

        elif ext == '.csv':
            document_content = []
            with open(data_file, newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    document_content.append(row)

        else:
            raise Exception(f"Unsupported file format: {ext}")

        json_data = {
            "document": document_content,
            "LINK": [
                {
                    "ARCHIV_ID": "DataSyncX1.0",
                    "AR_DATE": datetime.now().strftime("%Y%m%d"),
                    "AR_OBJECT": "DataSyncX1.0",
                    "ARC_DOC_ID": get_next_id("arc_doc_id", logger),
                    "RESERVE": "PDF",
                    "FILENAME": os.path.basename(pdf_file)
                }
            ],
            "INSTANCE": "DEV",
            "SESSION": "DataSyncX1.0",
            "ARCHIVEKEY": "DataSyncX1.0",
            "OBJECT": "DataSyncX1.0",
            "OFFSET": get_next_id("offset", logger),
            "ACTIVITY_ID": activity_id
        }

        logger.info(f"Data extraction and transformation successful for: {data_file}")
        return json_data

    except Exception as e:
        logger.error(f"Data extraction and transformation failed for: {data_file}. Error: {e}")
        logger.error(traceback.format_exc())
        return None

def initialize_cloudinary(config):
    """Initialize Cloudinary with credentials"""
    try:
        cloudinary.config(
            cloud_name = config['DEFAULT']['cloudinary_cloud_name'],
            api_key = config['DEFAULT']['cloudinary_api_key'],
            api_secret = config['DEFAULT']['cloudinary_api_secret']
        )
        # Test configuration
        cloudinary.api.ping()
        return True
    except Exception as e:
        print(f"Failed to initialize Cloudinary: {e}")
        return False

def upload_to_cloudinary(pdf_file, key, logger):
    try:
        # Upload the file
        result = cloudinary.uploader.upload(
            pdf_file,
            public_id=f'attachments/{key}',
            resource_type="auto",
            folder="datasyncx"  # Optional: organize files in folders
        )
        
        # Get the secure URL
        download_url = result['secure_url']
        
        logger.info(f"PDF file uploaded to Cloudinary: {pdf_file} at {download_url}")
        return download_url
    except Exception as e:
        logger.error(f"PDF file upload to Cloudinary failed for: {pdf_file}. Error: {e}")
        return None

def insert_into_mongodb(json_data, mongodb_uri, mongodb_database, mongodb_collection, logger):
    try:
        client = MongoClient(mongodb_uri)
        db = client[mongodb_database]
        collection = db[mongodb_collection]
        collection.insert_one(json_data)
        if db == "CDNDEV":
            logger.info("Json data has been inserted into MongoDB")
    except Exception as e:
        logger.error(f"Data insertion into MongoDB failed. Error: {e}")

def log_email(activity_id, email_content, status, recipient, logger):
    """
    Log email details to MongoDB
    
    Args:
        activity_id (str): The activity ID associated with the email
        email_content (str): Content of the email
        status (str): Status of the email (sent/failed)
        recipient (str): Email recipient
        logger: Logger instance
    """
    try:
        email_log = {
            "activity_id": activity_id,
            "email_content": email_content,
            "sent_timestamp": datetime.now(),
            "status": status,
            "recipient": recipient
        }
        
        # Use insert_into_mongodb instead of directly accessing log_db
        insert_into_mongodb(
            email_log,
            config['DEFAULT']['mongodb_uri'],
            config['DEFAULT']['mongodb_log_database'],
            config['DEFAULT']['mongodb_log_email_collection'],
            logger
        )
        logger.info(f"Email log saved successfully for activity {activity_id}")
        
    except Exception as e:
        logger.error(f"Failed to save email log for activity {activity_id}. Error: {e}")

# Update the send_email function to include logging
def send_email(subject, body, config, logger, activity_id=None):
    try:
        # Email setup
        smtp_server = config['DEFAULT']['smtp_server']
        smtp_port = config['DEFAULT']['smtp_port']
        smtp_user = config['DEFAULT']['smtp_username']
        smtp_password = config['DEFAULT']['smtp_password']
        sender_email = config['DEFAULT']['email_from']
        recipient_email = config['DEFAULT']['email_to']

        # Parse the body text into stats dictionary
        stats = dict(line.split(': ', 1) for line in body.split('\n') if ': ' in line)
        
        # Create email message
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg['Subject'] = subject

        # Add plain text and HTML parts
        msg.attach(MIMEText(body, 'plain'))
        msg.attach(MIMEText(create_html_content(stats), 'html'))

        # Send email
        server = smtplib.SMTP(smtp_server, int(smtp_port))
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        
        # Log successful email
        if activity_id:
            log_email(activity_id, body, "sent", recipient_email, logger)
            
        logger.info(f"Email sent successfully to {recipient_email}")
        server.quit()

    except Exception as e:
        if activity_id:
            log_email(activity_id, body, "failed", recipient_email, logger)
        logger.error(f"Failed to send email. Error: {e}")

def create_html_content(stats):
    # Calculate processing duration
    try:
        start_time = datetime.fromisoformat(stats.get('Activity Start Time', ''))
        end_time = datetime.fromisoformat(stats.get('Activity End Time', ''))
        duration = end_time - start_time
        duration_str = str(duration).split('.')[0]  # Remove microseconds
    except:
        duration_str = "N/A"

    # Calculate success rate
    try:
        total = int(stats.get('Total Files', 0))
        passed = int(stats.get('Passed Files', 0))
        failed_files = int(stats.get('Failed Files', 0))
        success_rate = (passed / total * 100) if total > 0 else 0
        total_files = total
    except:
        success_rate = 0
        failed_files = 0
        total_files = 0

    return f"""
        <html>
            <body style="font-family: 'Segoe UI', Arial, sans-serif; padding: 20px; margin: 0; background-color: #f5f5f5;">
                <div style="max-width: 600px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
                    <!-- Header Section -->
                    <div style="text-align: center; margin-bottom: 30px;">
                        <h1 style="color: #2c3e50; margin: 0; font-size: 24px;">
                            🔄 DataSyncX Processing Report
                        </h1>
                        <p style="color: #7f8c8d; margin: 10px 0 0 0;">
                            Generated on {datetime.now().strftime('%B %d, %Y at %H:%M:%S')}
                        </p>
                    </div>

                    <!-- Status Banner -->
                    <div style="background-color: {('#27ae60' if failed_files == 0 else '#e74c3c') if total_files > 0 else '#3498db'}; 
                              color: white; 
                              padding: 15px; 
                              border-radius: 8px; 
                              text-align: center; 
                              margin-bottom: 20px;">
                        <h2 style="margin: 0; font-size: 18px;">
                            {('✅ All Files Processed Successfully' if failed_files == 0 else '⚠️ Some Files Failed') if total_files > 0 else 'ℹ️ No Files Processed'}
                        </h2>
                    </div>

                    <!-- Activity Summary -->
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                        <h2 style="color: #34495e; font-size: 18px; margin: 0 0 15px 0;">
                            📊 Activity Summary
                        </h2>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Activity ID</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{stats.get('Activity ID', 'N/A')}</p>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Success Rate</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{success_rate:.1f}%</p>
                            </div>
                        </div>
                    </div>

                    <!-- Processing Statistics -->
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                        <h2 style="color: #34495e; font-size: 18px; margin: 0 0 15px 0;">
                            📈 Processing Statistics
                        </h2>
                        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; margin-bottom: 15px;">
                            <div style="text-align: left; padding: 10px; background: white; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Total Files</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">📁 {stats.get('Total Files', '0')}</p>
                            </div>
                            <div style="text-align: left; padding: 10px; background: white; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Passed</p>
                                <p style="color: #27ae60; margin: 0; font-weight: 500;">✅ {stats.get('Passed Files', '0')}</p>
                            </div>
                            <div style="text-align: left; padding: 10px; background: white; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Failed</p>
                                <p style="color: #e74c3c; margin: 0; font-weight: 500;">❌ {stats.get('Failed Files', '0')}</p>
                            </div>
                        </div>
                    </div>

                    <!-- Size Information -->
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                        <h2 style="color: #34495e; font-size: 18px; margin: 0 0 15px 0;">
                            📦 Size Information
                        </h2>
                        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">XML Size</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{format_size(stats.get('Total XML Size', 0))}</p>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">PDF Size</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{format_size(stats.get('Total PDF Size', 0))}</p>
                            </div>
                        </div>
                    </div>

                    <!-- Timing Information -->
                    <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                        <h2 style="color: #34495e; font-size: 18px; margin: 0 0 15px 0;">
                            ⏰ Processing Timeline
                        </h2>
                        <div style="display: grid; grid-template-columns: 1fr; gap: 10px;">
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Started</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{format_datetime(stats.get('Activity Start Time', 'N/A'))}</p>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Completed</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">{format_datetime(stats.get('Activity End Time', 'N/A'))}</p>
                            </div>
                            <div style="background: white; padding: 15px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
                                <p style="color: #7f8c8d; margin: 0 0 5px 0; font-size: 12px;">Duration</p>
                                <p style="color: #2c3e50; margin: 0; font-weight: 500;">⌛ {duration_str}</p>
                            </div>
                        </div>
                    </div>

                    <!-- Footer -->
                    <div style="text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee;">
                        <p style="color: #95a5a6; margin: 0; font-size: 12px;">
                            Generated automatically by DataSyncX System 🤖
                        </p>
                        <p style="color: #95a5a6; margin: 5px 0 0 0; font-size: 12px;">
                            For support, contact IT Service Desk
                        </p>
                    </div>
                </div>
            </body>
        </html>
    """

def format_size(size_input):
    """Convert bytes to human readable format"""
    try:
        # If size_input is a string, clean it up
        if isinstance(size_input, str):
            # Remove any non-digit characters except decimal points
            size_input = ''.join(c for c in size_input if c.isdigit() or c == '.')
            if not size_input:  # If string is empty after cleaning
                return "0 B"
        
        size_in_bytes = float(size_input)  # Use float instead of int
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_in_bytes < 1024:
                return f"{size_in_bytes:.2f} {unit}"
            size_in_bytes /= 1024
        return f"{size_in_bytes:.2f} TB"
    except (ValueError, TypeError, AttributeError):
        return "0 B"

def format_datetime(datetime_str):
    """Format datetime string to more readable format"""
    try:
        dt = datetime.fromisoformat(datetime_str)
        return dt.strftime('%B %d, %Y at %H:%M:%S')
    except:
        return datetime_str

def process_files(activity_id, logger):
    pickup_folder = f"{config['DEFAULT']['pickup_folder']}"
    processed_folder = f"{config['DEFAULT']['processed_folder']}"
    binary_folder = f"{config['DEFAULT']['binary_folder']}"
    error_folder = config['DEFAULT']['error_folder']
    

    total_files = 0
    passed_files = 0
    failed_files = 0
    total_xml_size = 0
    total_pdf_size = 0

    for filename in os.listdir(pickup_folder):
        base_name, ext = os.path.splitext(filename)
        pdf_file = os.path.join(pickup_folder, f"{base_name}.pdf")

        if ext in ['.xml', '.json', '.csv']:
            total_files += 1
            data_file = os.path.join(pickup_folder, filename)
            pair_logs = []
            dhr_id = get_next_id("dhr_id",logger)
            logger.info(f"Assigned DHR ID {dhr_id} for the pair: {base_name}")
            try:
                # Start timing the processing of the pair
                start_time = datetime.now()

                # Validation
                if not validate_files(data_file, pdf_file, logger):
                    raise Exception(f"Validation failed for pair: {data_file} and {pdf_file}")

                pair_logs.append(f"Validation successful for pair: {data_file} and {pdf_file}")

                # Extraction and Transformation
                json_data = extract_and_transform(data_file, pdf_file, activity_id, logger)
                if not json_data:
                    raise Exception(f"Data extraction and transformation failed for: {data_file}")

                pair_logs.append(f"Data extraction and transformation successful for: {data_file}")


                
                cloudinary_url = upload_to_cloudinary(pdf_file, base_name, logger)
                if not cloudinary_url:
                    raise Exception(f"Failed to upload to Cloudinary for: {pdf_file}")

                # Update the JSON data with the Cloudinary URL
                json_data["LINK"] = cloudinary_url

                pair_logs.append(f"PDF file uploaded to Cloudinary: {pdf_file} at {cloudinary_url}")

                # MongoDB Insertion
                insert_into_mongodb(json_data, config['DEFAULT']['mongodb_uri'], config['DEFAULT']['mongodb_database'], config['DEFAULT']['mongodb_collection'], logger)
                pair_logs.append("Json data has been inserted into MongoDB")

                # Updating Sizes and Moving Files
                xml_size = os.path.getsize(data_file)
                pdf_size = os.path.getsize(pdf_file)
                total_xml_size += xml_size
                total_pdf_size += pdf_size
                shutil.move(data_file, os.path.join(processed_folder, filename))
                shutil.move(pdf_file, os.path.join(processed_folder, f"{base_name}.pdf"))
                logger.info(f"Backed Up files {data_file} and {pdf_file} to processed folder")
                pair_logs.append(f"Backed Up {data_file} and {pdf_file} to processed folder")
                passed_files += 1

                # Log the size and processing time for the pair
                end_time = datetime.now()
                processing_time = (end_time - start_time).total_seconds()
                pair_logs.append(f"Pair processing time: {processing_time} seconds")
                logger.info(f"Pair processing time: {processing_time} seconds")
                pair_logs.append(f"XML size: {xml_size} bytes, PDF size: {pdf_size} bytes")
                logger.info(f"XML size: {xml_size} bytes, PDF size: {pdf_size} bytes")

                # Logging Pair History and S3 Info
                log_pair_history(activity_id, base_name, "Passed", end_time.isoformat(),dhr_id, logger)
                log_s3_info(activity_id, base_name, cloudinary_url,dhr_id, logger)

                logger.info("_________________________________________________________________________________________________________________________________")

            except Exception as e:
                logger.error(f"Error processing pair: {data_file} and {pdf_file}. Error: {e}")
                pair_logs.append(f"Error processing pair: {data_file} and {pdf_file}. Error: {e}")
                pair_logs.append(traceback.format_exc())
                log_pair_history(activity_id, base_name, "Failed", datetime.now().isoformat(),dhr_id, logger)
                failed_files += 1

                logger.info("_________________________________________________________________________________________________________________________________")

            log_events(activity_id, base_name, pair_logs,dhr_id, logger)

    return total_files, passed_files, failed_files, total_xml_size, total_pdf_size


def process_activity(activity_id):
    logger = setup_logger(activity_id)
    logger.info(f"Starting process activity with ID: {activity_id}")
    activity_start_time = datetime.now().isoformat()
    try:
        total_files, passed_files, failed_files, total_xml_size, total_pdf_size = process_files(activity_id, logger)
        activity_end_time = datetime.now().isoformat()
        log_activity_info(activity_id, total_files, passed_files, failed_files, total_xml_size, total_pdf_size, activity_start_time, activity_end_time, logger)
        logger.info(f"Process activity {activity_id} completed successfully.")

        # Prepare email content
        subject = f"DataSyncX-1.0 : Activity {activity_id} Completed"
        body = (f"Activity ID: {activity_id}\n"
                f"Total Files: {total_files}\n"
                f"Passed Files: {passed_files}\n"
                f"Failed Files: {failed_files}\n"
                f"Total XML Size: {total_xml_size} bytes\n"
                f"Total PDF Size: {total_pdf_size} bytes\n"
                f"Activity Start Time: {activity_start_time}\n"
                f"Activity End Time: {activity_end_time}\n")

        # Send email with activity_id
        send_email(subject, body, config, logger, activity_id)

    except Exception as e:
        logger.error(f"Process activity {activity_id} failed. Error: {e}")
        logger.error(traceback.format_exc())
        activity_end_time = datetime.now().isoformat()
        log_activity_info(activity_id, 0, 0, 0, 0, 0, activity_start_time, activity_end_time, logger)

        # Prepare email content for failure
        subject = f"Activity {activity_id} Failed"
        body = (f"Activity ID: {activity_id}\n"
                f"An error occurred during the processing.\n"
                f"Error: {e}\n"
                f"Activity Start Time: {activity_start_time}\n"
                f"Activity End Time: {activity_end_time}\n")

        # Send email with activity_id
        send_email(subject, body, config, logger, activity_id)


if __name__ == "__main__":
    activity_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Initialize Cloudinary before processing
    if initialize_cloudinary(config):
        process_activity(activity_id)
    else:
        print("Failed to initialize Cloudinary. Exiting...")
        sys.exit(1)

def update_props_file(config, section='DEFAULT', logger=None, **kwargs):
    """Update properties file with new values"""
    try:
        # Get the executable's directory for the properties file
        exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(__file__)
        props_path = os.path.join(exe_dir, 'props.properties')
        
        # Update the ConfigParser object
        for key, value in kwargs.items():
            config[section][key] = str(value)
        
        # Write to the properties file
        with open(props_path, 'w') as configfile:
            config.write(configfile)
            
        if logger:
            logger.info(f"Updated properties file with new values: {kwargs}")
    except Exception as e:
        if logger:
            logger.error(f"Failed to update properties file: {e}")
