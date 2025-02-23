# Processing-uploaded-files-in-AWS
This pipeline integrates several components to automatically handle uploaded files.

![image](https://github.com/user-attachments/assets/fce59c38-d759-43d3-9bed-2aeb0c5b9f73)

**Summary**

S3: Receives the upload and sends a message to the broker (EventBridge).

EventBridge: Acts as a trigger, triggering a workflow that will start the Glue Job.

SQS: Provides the file information (path, file type, etc.) that will be consumed by the Glue Job. Based on this information, the job will read the file and perform the necessary transformations.

Glue Job: Transforms the data and saves it to the second bucket (Parquet). It also produces the table in the Glue Data Catalog.

Athena: Performs queries on the data, via Catalog.

CloudWatch: Monitors metrics, logs and execution status.


**Configuration**

_S3 Bucket_

This S3 Bucket is the entry point of the pipeline, where the files should be inserted. 
In the "Properties" tab, "Event Notifications" must be configured to allow sending S3 notifications to EventBridge and SQS:

a) In "Create Notification", the following fields must be filled in: 
- Event Name; 
- File Suffix: (must be specified) 
- Event Type: Put (s3:ObjectCreated:Put) 
- Destination: SQS Queue (queue must be specified)

b) EventBridge must be set to "ON" 


_Eventbridge (Broker)_ 

EventBridge is the broker that receives notifications from the S3 Bucket. In this pipeline, it was used to act as a trigger for a Glue Workflow. 
Only the upload of new files (with file suffix specified) will be forwarded to Glue Workflow.

Target: EventBridge must be configured to point to the Glue Workflow.


_SQS (Queue Service)_

SQS is a messaging service provided through a queue. In this pipeline, the objective is to integrate with the Glue Job, so that it consumes the messages from the queue, thus obtaining the information of the file to be read and processed (bucket name, file name, folders, etc.). 

Configuration details: 
In the Access policy, 2 items are important: the resource name (the SQS queue name itself) and the ARN of the bucket that was configured to send S3 notifications to this queue. To check the settings, we must click on the "Edit" button of the SQS queue. 


_Glue Workflow_

Glue workflow is the tool that orchestrates the entire data reading and transformation flow, being automatically triggered by a trigger (EventBridge), and starting a Glue Job.  
The circle represents the trigger, while the rectangle refers to the Glue Job.


_Glue Job_

The Glue job can be a visual ETL or a programming code to perform tasks related to data ingestion, transformation and loading. In our case, the code was chosen to provide greater flexibility. Below are the job configuration details:

  The required IAM permissions are as follows:
  
  EventBridge Full Access: so that "trigger" events can trigger the job.
  
  S3 Full Access: to allow reading and writing of data in S3.
  
  Create-Catalog-databases-tables: to allow the job to interact with Databases and Tables of the Glue Data Catalog. 
  
  Datasource-read-write-sqs: to allow the glue job to access and consume information from the queue service. 
  
  Glue-publish-data-quality: integration with the Data Quality service. 
  
  Logs-Cloudwatch: communication with CloudWatch, to record events and tasks. 


_Bucket S3 (Data Target)_

For the destination of the data processed by the job (deduplication, merge between dataframes, etc.), a directory within the bucket used as source is being used. Details below: 
Bucket "source" / Bucket "target".

As explained in the Glue job item, all write and read permissions are already configured for this bucket and respective folders/files 


_Glue Data Catalog_

Through the Glue Job, Databases/Tables are created automatically, and are the representation of the metadata obtained from the uploaded file. 
The creation/update information is within the Glue Job code. 
In our case, the Database has a specific name, and the tables are produced with dynamic names, which are a composition of the names of the folders and files created.


_Cloudwatch Logs_

CloudWatch is configured to log activities and events from various components, but in this pipeline, its main purpose is to record the step-by-step execution of the Glue Job. 


_Querying data using Athena _

Through the Athena console, it is possible to consult tables with data processed from the original file, and perform SQL operations. 
To access, it is necessary to select the database (in our case, file-upload-source), and the tables will be displayed automatically. 
The details of each SQL operation will depend on what you want to execute.
