import os
import logging
import pandas as pd
from azure.storage.blob import BlobServiceClient
import azure.functions as func
from smartsheet import Smartsheet

# Initialize Smartsheet client
smartsheet_token = os.getenv('SMARTSHEET_ACCESS_TOKEN')
smartsheet_client = Smartsheet(smartsheet_token)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Fetch Smartsheet sheet ID from environment variable
        sheet_id_str = os.getenv('SMARTSHEET_ID')
        if sheet_id_str is None:
            raise ValueError("Environment variable SMARTSHEET_ID is not set")
        
        sheet_id = int(sheet_id_str)
        logging.info(f'Fetching sheet with ID: {sheet_id}')
        
        sheet = smartsheet_client.Sheets.get_sheet(sheet_id)

        # Extract data from Smartsheet and process it
        data_frames = []
        for row in sheet.rows:
            attachments = smartsheet_client.Sheets.get_row_attachments(sheet_id, row.id)
            for attachment in attachments.data:
                if attachment.name.endswith('.xlsm') or attachment.name.endswith('.xlsx'):
                    attachment_url = attachment.url
                    logging.info(f'Downloading attachment: {attachment.name}')
                    df = pd.read_excel(attachment_url, sheet_name=None)
                    for sheet_name, data in df.items():
                        if 'assembly' in data.columns and 'description' in data.columns:
                            data_frames.append(data)

        if data_frames:
            combined_df = pd.concat(data_frames, ignore_index=True)
            
            # Add a new column combining 'assembly' and 'description'
            combined_df['Formatted PON'] = combined_df.apply(lambda x: f"{x['assembly']}-{x['description']}", axis=1)
            
            # Upload to Azure Blob Storage
            blob_service_client = BlobServiceClient.from_connection_string(os.getenv('AZURE_STORAGE_CONNECTION_STRING'))
            blob_client = blob_service_client.get_blob_client(container=os.getenv('BLOB_CONTAINER_NAME'), blob='Latest_BOMS.csv')
            blob_client.upload_blob(combined_df.to_csv(index=False), overwrite=True)
            logging.info(f"Data processed and uploaded to Azure Blob Storage.")
        else:
            logging.info("No data frames were created. Please check if there are any attachments to process.")
        
        return func.HttpResponse("Data processed and uploaded successfully.", status_code=200)

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)


