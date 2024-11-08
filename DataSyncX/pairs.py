import os
import random
from faker import Faker
from fpdf import FPDF

# Initialize Faker
fake = Faker()

def generate_dummy_xml(file_path):
    # Generate random data
    name = fake.name()
    age = str(random.randint(1, 100))
    color = fake.color_name()

    # Format the XML content
    xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<document>
    <name>{name}</name>
    <age>{age}</age>
    <color>{color}</color>
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

    for i in range(12,12+num_files):
        file_name = f"file_{i+1}"
        xml_file_path = os.path.join(folder_path, f"{file_name}.xml")
        pdf_file_path = os.path.join(folder_path, f"{file_name}.pdf")

        generate_dummy_xml(xml_file_path)
        create_empty_pdf(pdf_file_path)

# Specify the folder path and number of files
folder_path = "C:\DataSyncX\pickup"
num_files = 20  # Number of XML and PDF pairs to generate

generate_files(folder_path, num_files)
