import os
import random
from faker import Faker
from fpdf import FPDF
from datetime import timedelta

# Initialize Faker
fake = Faker()

def generate_dummy_xml(file_path):
    # Generate random data
    batch_number = str(random.randint(100000, 999999))
    po_number = f"PO-{random.randint(10000, 99999)}"
    
    # Generate dates
    mfg_date = fake.date_between(start_date='-30d', end_date='today')
    expiry_date = mfg_date + timedelta(days=730)  # 2 years after manufacture
    
    # Generate other IDs
    serial_number = f"SN-{random.randint(100000, 999999)}"
    model_number = f"MDL-{str(random.randint(1, 999)).zfill(3)}"
    operator_id = f"OP-{str(random.randint(100, 999))}"
    facility_id = f"FAC-{str(random.randint(100, 999))}"
    
    # Quality results (90% pass rate)
    quality_result = "Pass" if random.random() < 0.9 else "Fail"
    qa_approval = "Approved by QA" if quality_result == "Pass" else "Rejected by QA"

    # Format the XML content
    xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<document>
    <batch_number>{batch_number}</batch_number>
    <production_order_number>{po_number}</production_order_number>
    <date_of_manufacture>{mfg_date}</date_of_manufacture>
    <expiry_date>{expiry_date}</expiry_date>
    <serial_number>{serial_number}</serial_number>
    <device_model_number>{model_number}</device_model_number>
    <operator_id>{operator_id}</operator_id>
    <manufacturing_facility_id>{facility_id}</manufacturing_facility_id>
    <quality_inspection_results>{quality_result}</quality_inspection_results>
    <final_release_approval>{qa_approval}</final_release_approval>
</document>"""

    # Write XML content to file
    with open(file_path, 'w') as file:
        file.write(xml_content)

def create_empty_pdf(file_path):
    pdf = FPDF()
    pdf.add_page()
    pdf.output(file_path)

def generate_files(folder_path, num_files):
    os.makedirs(folder_path, exist_ok=True)

    for i in range(0,num_files):
        file_name = f"file_{i+1}"
        xml_file_path = os.path.join(folder_path, f"{file_name}.xml")
        pdf_file_path = os.path.join(folder_path, f"{file_name}.pdf")

        generate_dummy_xml(xml_file_path)
        create_empty_pdf(pdf_file_path)

# Specify the folder path and number of files
folder_path = "C:\\DataSyncX\\pickup"
num_files = 5  # Number of XML and PDF pairs to generate

generate_files(folder_path, num_files)
