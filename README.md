# lithops-radix-sort

This application sorts data stored in the cloud. It uses Lithops framework which makes it easy to deploy to different clouds such as IBM, Google Cloud, Azure and AWS. 
However, it has only been tested on AWS using AWS Lambda for processing and S3 for storage.

# Build 

Build the included Dockerfile and upload to the appropriate cloud using the instructions at https://github.com/lithops-cloud/lithops/tree/master/runtime.

For AWS Lambda: lithops runtime build -f MyDockerfile docker_username/my_container_runtime -b aws_lambda.

Add config file ```.lithops_config```

# Run

Set the correct values for config variables at the top of ```main.py``` file.

Run the sorting: ```python main.py``` 
