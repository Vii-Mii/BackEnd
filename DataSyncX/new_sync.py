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
offset_counter_path = resource_path('offset_counter.txt')
arc_doc_id_counter_path = resource_path('arc_doc_id_counter.txt')
xsd_path = resource_path('document.xsd')

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

def update_properties(config, file_path):
    with open(file_path, 'w') as configfile:
        config.write(configfile)

def get_next_id(counter_name, logger):
    try:
        current_id = int(config['DEFAULT'][counter_name])
        next_id = current_id + 1
        config['DEFAULT'][counter_name] = str(next_id).zfill(16) if counter_name == "arc_doc_id" else str(next_id)
        update_properties(config, properties_path)
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

def read_properties(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config

config = read_properties(properties_path)

def validate_xml(xml_file, xsd_file, logger):
    try:
        with open(xsd_file, 'r') as xsd:
            schema_root = etree.XML(xsd.read())
        schema = etree.XMLSchema(schema_root)
        with open(xml_file, 'r') as xml:
            xml_doc = etree.parse(xml)
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
                    "ARCHIV_ID": "penang",
                    "AR_DATE": datetime.now().strftime("%Y%m%d"),
                    "AR_OBJECT": "penang",
                    "ARC_DOC_ID": get_next_id("arc_doc_id", logger),
                    "RESERVE": "PDF",
                    "FILENAME": os.path.basename(pdf_file)
                }
            ],
            "INSTANCE": "DEV",
            "SESSION": "penang",
            "ARCHIVEKEY": "penang",
            "OBJECT": "penang",
            "OFFSET": get_next_id("offset", logger),
            "ACTIVITY_ID": activity_id
        }

        logger.info(f"Data extraction and transformation successful for: {data_file}")
        return json_data

    except Exception as e:
        logger.error(f"Data extraction and transformation failed for: {data_file}. Error: {e}")
        logger.error(traceback.format_exc())
        return None

def upload_to_s3(pdf_file, base_name, bucket_name, s3_prefix, aws_access_key_id, aws_secret_access_key, logger, key):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        s3_key = os.path.join(s3_prefix, key+"_data")
        s3.upload_file(pdf_file, bucket_name, s3_key)
        s3_url = f"https://{bucket_name}.s3.amazonaws.com/{s3_key}"
        logger.info(f"PDF file uploaded to S3: {pdf_file} at {s3_url}")
        return s3_key
    except Exception as e:
        logger.error(f"PDF file upload to S3 failed for: {pdf_file}. Error: {e}")
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

def send_email(subject, body, config, logger):
    try:
        smtp_server = config['DEFAULT']['smtp_server']
        smtp_port = config['DEFAULT']['smtp_port']
        smtp_user = config['DEFAULT']['smtp_username']
        smtp_password = config['DEFAULT']['smtp_password']
        recipient_email = config['DEFAULT']['email_to']

        msg = MIMEMultipart()
        msg['From'] = smtp_user
        msg['To'] = recipient_email
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        logger.info(f"Connecting to SMTP server {smtp_server}:{smtp_port}")

        if smtp_port == '465':
            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            logger.info("Using SSL for secure connection")
        else:
            server = smtplib.SMTP(smtp_server, smtp_port)
            logger.info("Starting TLS for secure connection")
            server.starttls()

        server.login(smtp_user, smtp_password)

        logger.info("Logged in to SMTP server")
        text = msg.as_string()

        logger.info("Sending email")
        server.sendmail(smtp_user, recipient_email, text)

        logger.info(f"Email sent to {recipient_email} with subject: {subject}")
        server.quit()

    except Exception as e:
        logger.error(f"Failed to send email. Error: {e}")

def process_files(activity_id, logger):
    pickup_folder = f"{config['DEFAULT']['pickup_folder']}"
    processed_folder = f"{config['DEFAULT']['processed_folder']}"
    binary_folder = f"{config['DEFAULT']['binary_folder']}"
    error_folder = config['DEFAULT']['error_folder']
    aws_access_key_id = config['DEFAULT']['aws_access_key_id']
    aws_secret_access_key = config['DEFAULT']['aws_secret_access_key']
    aws_bucket_name = config['DEFAULT']['aws_bucket_name']
    aws_s3_prefix = config['DEFAULT']['aws_s3_prefix']

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

                # S3 Upload
                key = json_data["LINK"][0]["ARC_DOC_ID"]
                binary_file = os.path.join(binary_folder, key+"_data")
                shutil.copy(pdf_file, binary_file)
                s3_key = upload_to_s3(binary_file, base_name, aws_bucket_name, aws_s3_prefix, aws_access_key_id, aws_secret_access_key, logger, key)
                if not s3_key:
                    raise Exception(f"Failed to upload to S3 for: {pdf_file}")

                pair_logs.append(f"PDF file uploaded to S3: {pdf_file} at {s3_key}")

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
                log_s3_info(activity_id, base_name, s3_key,dhr_id, logger)

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
        subject = f"Activity {activity_id} Completed"
        body = (f"Activity ID: {activity_id}\n"
                f"Total Files: {total_files}\n"
                f"Passed Files: {passed_files}\n"
                f"Failed Files: {failed_files}\n"
                f"Total XML Size: {total_xml_size} bytes\n"
                f"Total PDF Size: {total_pdf_size} bytes\n"
                f"Activity Start Time: {activity_start_time}\n"
                f"Activity End Time: {activity_end_time}\n")

        # Send email
        send_email(subject, body, config, logger)

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
                f"Traceback: {traceback.format_exc()}\n"
                f"Activity Start Time: {activity_start_time}\n"
                f"Activity End Time: {activity_end_time}\n")

        # Send email
        send_email(subject, body, config, logger)


if __name__ == "__main__":
    activity_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    process_activity(activity_id)
