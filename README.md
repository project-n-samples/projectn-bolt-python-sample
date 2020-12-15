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