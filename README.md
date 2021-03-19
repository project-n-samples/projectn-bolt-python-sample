# AWS Lambda Function in Python for Bolt

Sample AWS Lambda Applications in Python that utilizes [Python SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-python)

### Requirements

- Python 3.0 or higher
- [Python SDK for Bolt](https://gitlab.com/projectn-oss/projectn-bolt-python)

### Build From Source

* Clone the `Python SDK for Bolt` repository and navigate inside it

```bash
git clone https://gitlab.com/projectn-oss/projectn-bolt-python.git

cd projectn-bolt-python
```

* Make sure you have the latest versions of `setuptools` and `wheel` installed:

```bash
python3 -m pip install --upgrade setuptools wheel
```

* Now run this command from the root of the `Python SDK for Bolt` repository:

```bash
python3 setup.py sdist bdist_wheel
```

* This command should output a lot of text and once completed should generate two files in the dist directory:
 (we are interested in the .whl file)

```bash
dist/
  example_pkg_YOUR_USERNAME_HERE-0.0.1-py3-none-any.whl
  example_pkg_YOUR_USERNAME_HERE-0.0.1.tar.gz
```

* Clone the sample repository into the `python-sdk-lambda` directory.

```bash
git clone https://gitlab.com/projectn-oss/projectn-bolt-python-sample.git python-sdk-lambda
```

* Run this command (from the root of the `Python SDK for Bolt` repository) to install the boto3 sdk into the
  `python-sdk-lambda` directory. At this point all your code dependencies (for the python sdk, and boto3 + botocore)
  should have been installed.

```bash
pip install --target ./python-sdk-lambda ./dist/bolt_python_sdk-1.0.0-py3-none-any.whl
```

* create deployment package

```bash
cd python-sdk-lambda

zip -r bolt-python-lambda-demo.zip .
```

### Deploy

* Deploy the function to AWS Lambda by uploading the deployment package (bolt-python-lambda-demo.zip)

```bash
aws lambda create-function \
    --function-name <function-name> \
    --runtime python3.8 \
    --zip-file fileb://bolt-python-lambda-demo.zip \
    --handler BoltS3OpsHandler.lambda_handler \
    --role <function-execution-role-ARN> \
    --environment "Variables={BOLT_URL=<Bolt-Service-Url>}" \
    --memory-size 128 \
    --timeout 20
```

### Usage

* The AWS Lambda function can be tested from the AWS Management Console by creating a test event and specifying its
  inputs in JSON format.
  
#### BoltS3OpsHandler

* BoltS3OpsHandler is the handler that is invoked by AWS Lambda to process an incoming event.


* BoltS3OpsHandler accepts the following input parameters as part of the event:
  * sdkType - Endpoint to which request is sent. The following values are supported:
    * S3 - The Request is sent to S3.
    * Bolt - The Request is sent to Bolt, whose endpoint is configured via 'BOLT_URL' environment variable
      
  * requestType - type of request / operation to be performed. The following requests are supported:
    * list_objects_v2 - list objects
    * list_buckets - list buckets
    * head_object - head object
    * head_bucket - head bucket
    * get_object - get object (md5 hash)
    * put_object - upload object
    * delete_object - delete object
      
  * bucket - bucket name
    
  * key - key name


* Following are examples of events, for various requests, that can be used to invoke the handler.
    * Listing first 1000 objects from Bolt bucket:
      ```json
        {"requestType": "list_objects_v2", "sdkType": "BOLT", "bucket": "<bucket>"}
      ```
    * Listing buckets from S3:
      ```json
      {"requestType": "list_buckets", "sdkType": "S3"}
      ```
    * Get Bolt object metadata (HeadObject):
      ```json
      {"requestType": "head_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```
    * Check if S3 bucket exists (HeadBucket):
      ```json
      {"requestType": "head_bucket","sdkType": "S3", "bucket": "<bucket>"}
      ```  
    * Retrieve object (its MD5 Hash) from Bolt:
      ```json
      {"requestType": "get_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```  
    * Upload object to Bolt:
      ```json
      {"requestType": "put_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>", "value": "<value>"}
      ```  
    * Delete object from Bolt:
      ```json
      {"requestType": "delete_object", "sdkType": "BOLT", "bucket": "<bucket>", "key": "<key>"}
      ```
      

#### BoltS3ValidateObjHandler

* BoltS3ValidateObjHandler is a handler that is invoked by AWS Lambda to process an incoming event for performing 
  data validation tests. To use this handler, change the handler of the Lambda function to 
  `BoltS3ValidateObjHandler.lambda_handler`


* BoltS3ValidateObjHandler accepts the following input parameters as part of the event:
  * bucket - bucket name
  
  * key - key name

* Following is an example of an event that can be used to invoke the handler.
  * Retrieve object(its MD5 hash) from Bolt and S3:
    
    If the object is gzip encoded, object is decompressed before computing its MD5.
    ```json
    {"bucket": "<bucket>", "key": "<key>"}
    ```
    
#### BoltS3PerfHandler

* BoltS3PerfHandler is a handler function that is invoked by AWS Lambda to process an incoming event
  for Bolt/S3 Performance testing. Before running this function, ensure that a source bucket has been crunched by
  `Bolt` with cleaner turned `OFF`. To use this handler, change the handler of the Lambda function to 
  `BoltS3PerfHandler.lambda_handler`
  

* BoltS3PerfHandler accepts the following input parameters as part of the event:
  * requestType - type of request / operation to be performed. The following requests are supported:
    * list_objects_v2 - list objects
    * get_object - get object
    * get_object_passthrough - get object (via passthrough) of unmonitored bucket
    * put_object - upload object
    * delete_object - delete object
    * all - put, get, delete, list objects (default request if none specified)
      
  * bucket - bucket name
    
* Following are examples of events, for various requests, that can be used to invoke the handler.
    * Measure List objects performance of Bolt / S3.
      ```json
      {"requestType": "list_objects_v2", "bucket": "<bucket>"}
      ```
    * Measure Get object performance of Bolt / S3.
      ```json
      {"requestType": "get_object", "bucket": "<bucket>"}
      ```
    * Measure Get object passthrough performance of Bolt.
      ```json
      {"requestType": "get_object_passthrough", "bucket": "<unmonitored-bucket>"}
      ```
    * Measure Put object performance of Bolt / S3.
      ```json
      {"requestType": "put_object", "bucket": "<bucket>"}
      ```
    * Measure Delete object performance of Bolt / S3.
      ```json
      {"requestType": "delete_object", "bucket": "<bucket>"}
      ```
    * Measure Put, Delete, Get, List objects performance of Bolt / S3.
      ```json
      {"requestType": "all", "bucket": "<bucket>"}
      ```
      