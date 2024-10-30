from fastapi import FastAPI, HTTPException
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO
import json

# Initialize FastAPI app
app = FastAPI()

# Set your Azure Blob Storage connection string
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=narula12storage;AccountKey=s8rUHL11ngvXxzJMatsIPT1UKaQsXMw61lKTTb7xA4bM2AawsFIpuf0I4Ty5rwsPpqg4t6IDGe6c+AStCavGIg==;EndpointSuffix=core.windows.net"

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)

@app.get("/get-file/{container_name}/{file_name}")
async def get_file(container_name: str, file_name: str):
    try:
        # Get container client
        container_client = blob_service_client.get_container_client(container_name)
        
        # Get blob client for the specified file
        blob_client = container_client.get_blob_client(file_name)
        
        # Download blob content
        blob_data = blob_client.download_blob().readall()
        
        # Load blob data into a DataFrame
        df = pd.read_csv(StringIO(blob_data.decode('utf-8')))
        
        # Convert DataFrame to JSON
        data_json = df.to_json(orient="records")
        
        return json.loads(data_json)  # Return as JSON-compatible dict

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
