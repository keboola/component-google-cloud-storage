#Google Cloud Storage Writer

This component allows you to upload files and tables to Google Cloud Storage and if necessary append a timestamp 
to the file name to keep historical versions of files.

**Table of contents:**  
  
[TOC]

# Configuration

## Authorization
To authorize the writer you can either choose to use a Google Service account or use instant authorization
The Service account login is the recommended way of authorizing this component. 

- **Service Account Key** - JSON service account key downloaded from GCP

## Row Coniguration

- **Bucket name** - (REQ) Name of bucket in GCS
- **Append timestamp** - (OPT) Append timestamp to file name

**Example:**

```json
{
  "parameters": {
    "bucket_name": "cloud_writer_testing",
    "append_date": 1,
    "#service_account_key": "{JSON KEY HERE}"
  }
}
```


