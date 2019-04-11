# AzBlobUpload
.NET Core 2.1 tool for uploading blobs to Azure with rudimentary, yet effective, restart capabilities.

# Usage

## Parameters File

Format:

```json
{
    "storageConnectionString": "<storage connection string>",
    "containerName": "<storage container name>",
    "sourceFile": "<source file full path>"
}
```

Example:

```json
{
    "storageConnectionString": "AReallyLongStringFromAzureInYourStorageAccount",
    "containerName": "MyReallyCoolStorageContainer",
    "sourceFile": "MyFileIWantToUpload.mdf"
}
```

## Command Line

`> .\AzBlobUpload C:\pathtomyparameterfile\myparameterfile.json`

# Restart Mode

If your upload fails, you will see a small file next to your upload file with the `azrestart` extension. All you need to do is to issue the above command line and it should pick up with the last block the upload was trying to complete. If you want to start all over, just delete this file.
