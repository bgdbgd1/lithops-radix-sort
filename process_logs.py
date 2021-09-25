import datetime
import json
import os
import re
from time import sleep
import gzip
import shutil
import glob
import boto3
from slugify import slugify

EXPERIMENT_NAME = ''
EXECUTE_DOWNLOAD_LOGGING = True
EXECUTE_PROCESS_LOGGING = True

if EXECUTE_DOWNLOAD_LOGGING:
    # DOWNLOAD lithops.jobs from s3. We are interested in status.json
    print("Download lithops.jobs")
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('bogdan-experiments')
    for obj in bucket.objects.filter(Prefix='lithops.jobs'):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key)  # save to same path

    # DOWNLOAD CloudWatch logs
    tasks_ids = []

    logs_client = boto3.client('logs')
    log_groups = logs_client.describe_log_groups()
    LOGS_PREFIX = 'lithops_sorting_logs'

    print("Create Tasks.")
    for log_group in log_groups['logGroups']:
        timestamp_to = datetime.datetime.timestamp(
            datetime.datetime.fromtimestamp(int(log_group["creationTime"])/1000) + datetime.timedelta(days=1)
        )

        export_task = logs_client.create_export_task(
            taskName=f'task-{log_group["logGroupName"]}',
            logGroupName=log_group["logGroupName"],
            fromTime=int(log_group["creationTime"]),
            to=int(timestamp_to)*1000,
            destination='bogdan-experiments',
            destinationPrefix=LOGS_PREFIX
        )

        tasks_ids.append(export_task['taskId'])

    # Check if export tasks completed
    print("Start checking if tasks completed.")
    all_tasks_completed = False
    while not all_tasks_completed:
        tasks = logs_client.describe_export_tasks()
        one_uncompleted = False
        for task in tasks['exportTasks']:
            if task['status']['code'] != 'COMPLETED':
                one_uncompleted = True

        if not one_uncompleted:
            all_tasks_completed = True

        else:
            print("sleeping 20 seconds")
            sleep(20)  # seconds

    # # Download exported files
    print("Download exported log files")
    for obj in bucket.objects.filter(Prefix=LOGS_PREFIX):
        if not os.path.exists(os.path.dirname(obj.key)):
            os.makedirs(os.path.dirname(obj.key))
        bucket.download_file(obj.key, obj.key)  # save to same path

    # List files
    gz_files = glob.glob("/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/*.gz")

    # Unzip GZ files
    print("Unzip GZ Files")
    for zip_file in gz_files:
        dest_file_name = zip_file.split('.gz')[0]
        with gzip.open(zip_file, 'rb') as f_in:
            with open(dest_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
else:
    print("Skip download logging.")

if EXECUTE_PROCESS_LOGGING:
    # ------------------- PROCESS DATA ----------------------

    data = {}

    # Process Lithops Jobs
    print("Process Lithops Jobs")
    jobs_dirs = glob.glob("/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops.jobs/*M*")
    for job_dir in jobs_dirs:
        status_dirs = glob.glob(f'{job_dir}/*')
        for status_dir in status_dirs:
            with open(f'{status_dir}/status.json', 'r') as fp:
                status = json.load(fp)
            data.update({status['activation_id']: status})

    # Process main logging
    print("Process main handler logging")
    with open('main_logging.txt', 'r') as main_logging_file:
        for line in main_logging_file.readlines():
            # Get invocation time of each function run
            if 'invoked' in line and 'Activation ID' in line:
                result_invocation_time = re.search('invoked \((.*)\) - Activation ID: (.*)\n', line)
                invocation_time = result_invocation_time.group(1)
                activation_id = result_invocation_time.group(2)
                data.get(activation_id).update({"invocation_time": invocation_time})

            # Get activation id and total time
            if 'Activation ID' in line and 'Time: ' in line:
                result_activation_id = re.search('Activation ID: (.*) - Time: (.*)\n', line)
                activation_id = result_activation_id.group(1)
                total_time = result_activation_id.group(2)
                data.get(activation_id).update({'total_time': total_time})

    # Process CloudWatch Logging
    print("Process CloudWatch logging")
    key_phrases = [
        "DETERMINE CATEGORIES PHASE",
        "Start downloading initial file",
        "Finish downloading initial file",
        "Start first sorting",
        "Finish first sorting",
        "Start uploading first sorted file",
        "Finish 'determine categories' phase",
        "SORT CATEGORY PHASE",
        "Start download interval file",
        "Finish download interval file",
        "Start sort final file",
        "Finish sort final file",
        "Start write final file",
        "Finish write final file"
    ]
    lithops_sorting_logs_dirs = glob.glob('/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops_sorting_logs/*')
    for logs_dir in lithops_sorting_logs_dirs:
        if 'aws-logs-write-test' in logs_dir:
            continue
        activations_logs_dirs = glob.glob(f'{logs_dir}/*')
        print("Unzip lithops_sorting_logs_dirs GZ Files")
        for activation_dir in activations_logs_dirs:
            zip_file = f'{activation_dir}/000000.gz'
            dest_file_name = f'{activation_dir}/000000'
            with gzip.open(zip_file, 'rb') as f_in:
                with open(dest_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            print("Finish unzipping. Start processing")

            with open(dest_file_name, 'r') as log_file:
                request_data = {}
                for line in log_file.readlines():
                    if "START RequestId" in line:
                        request_data.update({'id': re.search("START RequestId: (.*) Version", line).group(1)})
                    elif "REPORT RequestId" in line:
                        results = re.search("Duration: (.*)\tBilled Duration: (.*)\tMemory Size", line)
                        execution_duration = results.group(1)
                        billed_duration = results.group(2)
                        results_init_duration = re.search("Init Duration: (.*)", line)
                        init_duration = results_init_duration.group(1)
                        request_data.update(
                            {
                                "execution_duration": execution_duration,
                                "billed_duration": billed_duration,
                                "init_duration": init_duration
                            }
                        )
                    elif "Start download interval file" in line:
                        results = re.search("(.*)Z Start download interval file (.*)\.", line)
                        timestamp = results.group(1)
                        file_name = results.group(2)
                        request_data.update({f"start_download_interval_file_{file_name}": timestamp})
                    elif "Finish download interval file" in line:
                        results = re.search("(.*)Z Finish download interval file (.*)\.", line)
                        timestamp = results.group(1)
                        file_name = results.group(2)
                        request_data.update({f"finish_download_interval_file_{file_name}": timestamp})
                    else:
                        for phrase in key_phrases:
                            if phrase in line:
                                request_data.update({f'{slugify(phrase, separator="_")}': re.search("(.*)Z ", line).group(1)})

                for key, value in request_data.items():
                    if key == 'id':
                        continue

                    data.get(request_data['id']).update({key: value})

    json_file = glob.glob(f"/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/experiments_raw_data/{EXPERIMENT_NAME}.json")
    if json_file:
        resp = input(f"The file {EXPERIMENT_NAME} already exists. Do you want to override it? y/new file name")
        if resp == 'y':
            pass
        else:
            EXPERIMENT_NAME = resp

    with open(f'experiments_raw_data/{EXPERIMENT_NAME}.json', 'w') as file:
        json.dump(data, file)
print("DONE")

