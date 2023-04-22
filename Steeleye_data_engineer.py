#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install -r requirements.txt


# In[2]:


import os
import time
import csv
import logging
from io import BytesIO
from datetime import datetime
from typing import List
from xml.etree.ElementTree import Element
from dotenv import load_dotenv
from lxml import etree
from zipfile import ZipFile
import requests
import boto3
import ntplib
from time import ctime

#logging.basicConfig(filename="app.log", level=logging.INFO)


def sync_time(ntp_server='pool.ntp.org'):
    """
    Synchronizes the local system time with an NTP server.

    Args:
        ntp_server (str): The NTP server URL to synchronize with. Defaults to 'pool.ntp.org'.

    Returns:
        bool: True if the synchronization was successful, False otherwise.
    """
    # Set up logging
    logger = logging.getLogger(__name__)

    # Connect to the NTP server and handle any exceptions
    try:
        client = ntplib.NTPClient()
        response = client.request(ntp_server)
    except ntplib.NTPException as e:
        logger.error('Unable to connect to NTP server: %s', e)
        return False

    # Adjust the local system time based on the NTP response
    if response:
        # Set the system clock to the NTP-synchronized time
        import os
        try:
            os.system('date -s "{}"'.format(ctime(response.tx_time)))
            logger.info('Time synchronized with NTP server')
            return True
        except OSError as e:
            logger.error('Error setting system time: %s', e)
            return False
    else:
        logger.warning('Unable to synchronize with NTP server.')
        return False


def load_env():
    load_dotenv()
    XML_URL = os.getenv('XML_URL')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    accesskey = os.getenv('AWS_ACCESS_KEY_ID')
    secret_access = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    return XML_URL, bucket_name, accesskey, secret_access

def download_xml_file(url: str) -> bytes:
    """
    Download an XML file from the given URL.

    Parameters:
        url (str): The URL of the file to download.

    Returns:
        bytes: The contents of the downloaded file.
    """
    response = requests.get(url)
    response.raise_for_status()

    return response.content

def extract_dltins_url(xml_content: bytes) -> str:
    """
    Extract the URL of the DLTINS file from the given XML content.

    Parameters:
        xml_content (bytes): The contents of the XML file.

    Returns:
        str: The URL of the DLTINS file.
    """
    root = etree.fromstring(xml_content)

    # Find the doc element with the DLTINS file type
    doc_xpath = ".//doc[str[@name='file_type']='DLTINS']"

    dltins_doc = root.xpath(doc_xpath)
    if not dltins_doc:
        logging.error(f"No DLTINS download link found in XML:\n{xml_content}")
        raise ValueError("No DLTINS download link found in XML")

    # Extract the download link from the DLTINS doc element
    download_link_xpath = ".//str[@name='download_link']"
    download_link_elem = dltins_doc[0].xpath(download_link_xpath)

    if not download_link_elem:
        logging.error(f"No download link found in DLTINS doc element:\n{etree.tostring(dltins_doc[0])}")
        raise ValueError("No DLTINS download link found in XML")

    return download_link_elem[0].text



def download_dltins_zip(url: str) -> bytes:
    """
    Download the DLTINS zip file from the provided URL.

    Parameters:
        url (str): The URL of the DLTINS zip file.

    Returns:
        bytes: The contents of the DLTINS zip file.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred while downloading zip: {http_err}")
        raise
    except Exception as err:
        logging.error(f"An error occurred while downloading zip: {err}")
        raise

def extract_xml_content(zip_content: bytes) -> bytes:
    """
    Extract the XML content from the given DLTINS zip file.

    Parameters:
        zip_content (bytes): The contents of the zip file.

    Returns:
        bytes: The contents of the XML file.
    """
    try:
        with ZipFile(BytesIO(zip_content)) as myzip:
            for filename in myzip.namelist():
                if filename.endswith(".xml"):
                    return myzip.read(filename)
    except Exception as e:
        logging.error(f"Error extracting XML content from zip file: {e}")
        raise ValueError("Error extracting XML content from zip file")


def convert_to_csv(xml_content: bytes) -> str:
    """
    Convert the given XML content into a CSV string.

    Parameters:
        xml_content (bytes): The contents of the XML file.

    Returns:
        str: The CSV string.
    """
    # Define the namespace dictionary
    ns = {
        "ns": "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02"
    }

    root = etree.fromstring(xml_content)

    # Define the fieldnames for the CSV file
    fieldnames = ["FinInstrmGnlAttrbts.Id", "FinInstrmGnlAttrbts.FullNm", "FinInstrmGnlAttrbts.ClssfctnTp",
                  "FinInstrmGnlAttrbts.CmmdtyDerivInd", "FinInstrmGnlAttrbts.NtnlCcy", "Issr"]
    csv_rows = [fieldnames]

    # Find all FinInstrm elements in the XML and extract the required data
    fininstrms = root.xpath("//ns:FinInstrm", namespaces=ns)
    for fininstrm in fininstrms:
        instrm_dict = {}
        id_elem = fininstrm.find(".//ns:Id", namespaces=ns)
        if id_elem is not None:
            instrm_dict["FinInstrmGnlAttrbts.Id"] = id_elem.text

        fullnm_elem = fininstrm.find(".//ns:FullNm", namespaces=ns)
        if fullnm_elem is not None:
            instrm_dict["FinInstrmGnlAttrbts.FullNm"] = fullnm_elem.text

        clssfctntp_elem = fininstrm.find(".//ns:ClssfctnTp", namespaces=ns)
        if clssfctntp_elem is not None:
            instrm_dict["FinInstrmGnlAttrbts.ClssfctnTp"] = clssfctntp_elem.text

        cmmdtyderivind_elem = fininstrm.find(".//ns:CmmdtyDerivInd", namespaces=ns)
        if cmmdtyderivind_elem is not None:
            instrm_dict["FinInstrmGnlAttrbts.CmmdtyDerivInd"] = cmmdtyderivind_elem.text

        ntnlccy_elem = fininstrm.find(".//ns:NtnlCcy", namespaces=ns)
        if ntnlccy_elem is not None:
            instrm_dict["FinInstrmGnlAttrbts.NtnlCcy"] = ntnlccy_elem.text

        issr_elem = fininstrm.find(".//ns:Issr", namespaces=ns)
        if issr_elem is not None:
            instrm_dict["Issr"] = issr_elem.text

        # Append the data for this FinInstrm to the list of rows
        csv_rows.append([instrm_dict.get(key, "") for key in fieldnames])

    # Join the rows into a CSV string and return it
    return "\n".join([",".join(row) for row in csv_rows])


def write_to_csv_and_upload_to_s3(csv_string: str, s3_client: boto3.client, bucket_name: str) -> None:
    """
    Write the given CSV string to a file and upload it to S3.

    Parameters:
        csv_string (str): The CSV string.
        s3_client (boto3.client): The S3 client object.
        bucket_name (str): The name of the S3 bucket to upload the file to.
    """
    try:
        now = datetime.now()
        dt_string = now.strftime("%Y-%m-%d_%H-%M-%S")
        filename = f"output_{dt_string}.csv"  # append timestamp to filename

        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            for row in csv_string.split('\n'):
                writer.writerow(row.split(','))

        s3_client.upload_file(filename, bucket_name, filename)

        # delete the local file after uploading to S3
        os.remove(filename)
    except Exception as e:
        logging.error(f"An error occurred while writing to CSV or uploading to S3: {e}")
        raise
        
def delete_old_files() -> None:
    """
    Delete all CSV files except for the latest one and the DLTINS zip file.
    """
    try:
        dt_string = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        filename_pattern = f"output_{dt_string}.csv"
        for file in os.listdir():
            if file.startswith("output_") and file.endswith(".csv") and file != filename_pattern or file == "dltins.zip":
                os.remove(file)
                logging.info(f"Deleted file: {file}")
    except OSError as e:
        logging.error(f"Error occurred while deleting files: {e}")
        raise
def main() -> None:
    """
    Main function that orchestrates the entire process.
    """
    try:
        # Call the sync_time() function to synchronize the system time
        logging.basicConfig(filename="app.log", level=logging.INFO)

        # Call the sync_time() function to synchronize the system time
        if sync_time():
            logging.info('System time successfully synchronized with NTP server.')
        else:
            logging.warning('System time synchronization failed.')

        # Step In: Load environment variables from .env file
        XML_URL, bucket_name, accesskey, secret_access = load_env()
        
        # Step 1: Download the XML file from the provided link
        xml_content = download_xml_file(XML_URL)
        
        # Step 2: Parse through the XML file to find the URL of the DLTINS file
        dltins_url = extract_dltins_url(xml_content)

        # Step 3: Download the zip file from the URL obtained in step 2
        zip_content = download_dltins_zip(dltins_url)

        # Step 4: Extract the XML file from the zip
        xml_content = extract_xml_content(zip_content)

        # Step 5: Convert XML contents into CSV file
        csv_string = convert_to_csv(xml_content)

        # Step 6: Write the data to a csv file and uploading it to S3 bucket
        #s3_client = boto3.client('s3')
        s3_client = boto3.client('s3', aws_access_key_id=accesskey, aws_secret_access_key=secret_access)
        write_to_csv_and_upload_to_s3(csv_string, s3_client, bucket_name)

        # Step 7: Wait for 10 seconds
        time.sleep(10)

        # Step 8: Delete old files
        delete_old_files()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise
if __name__ == '__main__':
    main()


# In[ ]:




