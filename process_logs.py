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


def update_request_data(data, worker_id, key_val):
    if data.get(worker_id):
        data[worker_id].update(
            key_val

        )
    else:
        data.update(
            {
                worker_id: key_val
            }
        )
    return data


def convert_string_ms_to_float_sec(duration):
    if ' ms\t' in duration:
        res = duration.split(' ms\t')
    elif ' ms' in duration:
        res = duration.split(' ms')

    return float("{:.2f}".format(float(res[0]) / 1000.0))


def process_logs(
        experiment_config,
        nr_intervals,
        EXECUTE_DOWNLOAD_LOGGING=True,
        EXECUTE_UNZIP=True,
        EXECUTE_PROCESS_LOGGING=True,
        REMOVE_CLOUDWATCH_LOGGING=False,
        REMOVE_S3_LOGGING=False,
        REMOVE_LOCAL_LOGGING=False
):
    BUCKET_NAME = 'bogdan-experiments'
    LOGS_PREFIX = f'lithops_sorting_logs_{experiment_config}_{nr_intervals}'
    nr_report_request_id_found = 0
    nr_report_without_init_time_found = 0

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(BUCKET_NAME)
    logs_client = boto3.client('logs')
    log_groups = None

    if EXECUTE_DOWNLOAD_LOGGING:
        # DOWNLOAD lithops.jobs from s3. We are interested in status.json
        # print("Download lithops.jobs")
        # for obj in bucket.objects.filter(Prefix='lithops.jobs'):
        #     if not os.path.exists(os.path.dirname(obj.key)):
        #         os.makedirs(os.path.dirname(obj.key))
        #     bucket.download_file(obj.key, obj.key)  # save to same path

        # DOWNLOAD CloudWatch logs
        tasks_ids = []

        log_groups = logs_client.describe_log_groups()
        tasks = logs_client.describe_export_tasks()

        print("Create Tasks.")
        for log_group in log_groups['logGroups']:
            timestamp_to = datetime.datetime.timestamp(
                datetime.datetime.fromtimestamp(int(log_group["creationTime"]) / 1000) + datetime.timedelta(days=1)
            )

            export_task = logs_client.create_export_task(
                taskName=f'task-{log_group["logGroupName"]}',
                logGroupName=log_group["logGroupName"],
                fromTime=int(log_group["creationTime"]),
                to=int(timestamp_to) * 1000,
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

    if EXECUTE_UNZIP:
        # List files
        gz_files = glob.glob("/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/*.gz")

        # Unzip GZ files
        print("Unzip GZ Files")
        for zip_file in gz_files:
            dest_file_name = zip_file.split('.gz')[0]
            with gzip.open(zip_file, 'rb') as f_in:
                with open(dest_file_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

    if EXECUTE_PROCESS_LOGGING:
        # ------------------- PROCESS DATA ----------------------

        data = {}

        # Process futures stats
        print("Read stats data")
        stats_files = glob.glob(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/stats/stats_logging_{experiment_config}_ExpNr_*_intervals_{nr_intervals}.json')
        for stats_file in stats_files:
            with open(stats_file, "r") as read_file:
                data.update(json.load(read_file))
        # Process Lithops Jobs
        # print("Process Lithops Jobs")
        # jobs_dirs = glob.glob("/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops.jobs/*M*")
        # for job_dir in jobs_dirs:
        #     status_dirs = glob.glob(f'{job_dir}/*')
        #     for status_dir in status_dirs:
        #         with open(f'{status_dir}/status.json', 'r') as fp:
        #             status = json.load(fp)
        #             status.pop('logs')
        #         data.update({status['activation_id']: status})

        # Process main logging
        # print("Process main handler logging")
        # with open('main_logging.txt', 'r') as main_logging_file:
        #     for line in main_logging_file.readlines():
        #         # Get invocation time of each function run
        #         if 'invoked' in line and 'Activation ID' in line:
        #             result_invocation_time = re.search('invoked \((.*)\) - Activation ID: (.*)\n', line)
        #             invocation_time = result_invocation_time.group(1)
        #             activation_id = result_invocation_time.group(2)
        #             data.get(activation_id).update({"invocation_time": invocation_time})
        #
        #         # Get activation id and total time
        #         if 'Activation ID' in line and 'Time: ' in line:
        #             result_activation_id = re.search('Activation ID: (.*) - Time: (.*)\n', line)
        #             activation_id = result_activation_id.group(1)
        #             total_time = result_activation_id.group(2)
        #             data.get(activation_id).update({'total_time': total_time})

        # Process CloudWatch Logging
        print("Process CloudWatch logging")
        key_phrases = [
            "Start 'determine categories' phase",
            "Start downloading initial file",
            "Finish downloading initial file",
            "Start first sorting",
            "Finish first sorting",
            "Start uploading first sorted file",
            "Finish uploading first sorted file",
            "Finish 'determine categories' phase",
            "Start 'sort_category' phase",
            "Finish 'sort_category' phase",
            "Start download intervals",
            "Finish download intervals",
            "Start sort final file",
            "Finish sort final file",
            "Start write final file",
            "Finish write final file"
        ]

        download_interval_files_phrases = [
            "Start download interval file",
            "Finish download interval file"
        ]

        if not os.path.isdir(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops_sorting_logs_{experiment_config}_{nr_intervals}/'):
            os.mkdir(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops_sorting_logs_{experiment_config}_{nr_intervals}/')
        lithops_sorting_logs_dirs = glob.glob(
            f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops_sorting_logs_{experiment_config}_{nr_intervals}//*')

        for logs_dir in lithops_sorting_logs_dirs:
            if 'aws-logs-write-test' in logs_dir:
                continue
            activations_logs_dirs = glob.glob(f'{logs_dir}/*')
            print("Unzip lithops_sorting_logs_dirs GZ Files")
            for activation_dir in activations_logs_dirs:
                if not os.path.isfile(f'{activation_dir}/000000'):
                    zip_file = f'{activation_dir}/000000.gz'
                    dest_file_name = f'{activation_dir}/000000'
                    with gzip.open(zip_file, 'rb') as f_in:
                        with open(dest_file_name, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    print("Finish unzipping. Start processing")
                else:
                    dest_file_name = f'{activation_dir}/000000'
                    # print("Skipped unzipping")

                with open(dest_file_name, 'r') as log_file:
                    for line in log_file.readlines():
                        if "START RequestId" in line:
                            results = re.search("(.*) START RequestId: (.*) Version", line)
                            dt = results.group(1)
                            dt_datetime = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
                            dt_timestamp = datetime.datetime.timestamp(dt_datetime)
                            worker_id = results.group(2)
                            data = update_request_data(data, worker_id, {'start_request': dt})
                            data = update_request_data(data, worker_id, {'start_request_timestamp': dt_timestamp})
                        elif "REPORT RequestId" in line:
                            nr_report_request_id_found += 1
                            results = re.search(
                                "REPORT RequestId: (.*)\tDuration: (.*)\tBilled Duration: (.*)\tMemory Size", line)
                            worker_id = results.group(1)
                            execution_duration = results.group(2)
                            billed_duration = results.group(3)
                            execution_duration_float = convert_string_ms_to_float_sec(execution_duration)
                            billed_duration_float = convert_string_ms_to_float_sec(billed_duration)
                            results_init_duration = re.search("Init Duration: (.*)\t", line)
                            try:
                                init_duration = results_init_duration.group(1)
                                init_duration_float = convert_string_ms_to_float_sec(init_duration)
                            except:
                                init_duration = 0
                                init_duration_float = 0
                                nr_report_without_init_time_found += 1

                            data = update_request_data(data, worker_id, {
                                "execution_duration": execution_duration,
                                "execution_duration_float": execution_duration_float,
                                "billed_duration": billed_duration,
                                "billed_duration_float": billed_duration_float,
                                "init_duration": init_duration,
                                "init_duration_float": init_duration_float
                            })
                        elif "Start download interval file" in line:
                            results = re.search("(.*) \[WORKER (.*)] Start download interval file (.*)\.", line)
                            dt = results.group(1)
                            dt_datetime = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
                            dt_timestamp = datetime.datetime.timestamp(dt_datetime)
                            worker_id = results.group(2)
                            file_name = results.group(3)
                            data = update_request_data(
                                data, worker_id, {f"start_download_interval_file_{file_name}": dt})
                            data = update_request_data(
                                data, worker_id, {f"start_download_interval_file_{file_name}_timestamp": dt_timestamp})
                        elif "Finish download interval file" in line:
                            results = re.search("(.*) \[WORKER (.*)] Finish download interval file (.*)\.", line)
                            dt = results.group(1)
                            dt_datetime = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
                            dt_timestamp = datetime.datetime.timestamp(dt_datetime)
                            worker_id = results.group(2)
                            file_name = results.group(3)
                            data = update_request_data(
                                data, worker_id, {f"finish_download_interval_file_{file_name}": dt})
                            data = update_request_data(
                                data, worker_id, {f"finish_download_interval_file_{file_name}_timestamp": dt_timestamp})
                        elif "worker_id=" in line and "function_id=" in line:
                            results = re.search(
                                "worker_id=(.*);function_id=(.*);experiment_config=(.*);experiment_number=(.*);", line)
                            worker_id = results.group(1)
                            function_id = int(results.group(2))
                            experiment_config = results.group(3)
                            experiment_number = results.group(4)
                            data = update_request_data(
                                data,
                                worker_id,
                                {
                                    "function_id": function_id,
                                    "experiment_config": experiment_config,
                                    "experiment_number": experiment_number
                                }
                            )
                        else:
                            for phrase in download_interval_files_phrases:
                                if phrase in line:
                                    results = re.search(f"(.*) \[WORKER (.*)] {phrase} (.*)\.", line)
                                    dt = results.group(1)
                                    dt_datetime = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
                                    dt_timestamp = datetime.datetime.timestamp(dt_datetime)
                                    worker_id = results.group(2)
                                    file_name = results.group(3)
                                    data = update_request_data(
                                        data, worker_id, {f'{slugify(phrase, separator="_")}_{file_name}': dt})
                                    data = update_request_data(
                                        data, worker_id,
                                        {f'{slugify(phrase, separator="_")}_{file_name}_timestamp': dt_timestamp})
                            for phrase in key_phrases:
                                if phrase in line:
                                    re_search = re.search(f"(.*) \[WORKER (.*)] {phrase}", line)
                                    dt = re_search.group(1)
                                    dt_datetime = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
                                    dt_timestamp = datetime.datetime.timestamp(dt_datetime)
                                    worker_id = re_search.group(2)
                                    data = update_request_data(
                                        data, worker_id, {slugify(phrase, separator="_"): dt})
                                    data = update_request_data(
                                        data, worker_id, {f'{slugify(phrase, separator="_")}_timestamp': dt_timestamp})

        data_per_experiment = {}
        no_exp_config_found = []
        for worker_id, values in data.items():
            if not values.get('experiment_config'):
                no_exp_config_found.append({worker_id: values})
                continue
            experiment_config = values['experiment_config']
            experiment_number = values['experiment_number']
            if data_per_experiment.get(experiment_config):
                if data_per_experiment[experiment_config].get(experiment_number):
                    data_per_experiment[experiment_config][experiment_number].update(
                        {
                            worker_id: values
                        }
                    )
                else:
                    data_per_experiment[experiment_config].update(
                        {
                            experiment_number: {
                                worker_id: values
                            }
                        }
                    )
            else:
                data_per_experiment.update(
                    {
                        experiment_config: {
                            experiment_number: {
                                worker_id: values
                            }
                        }
                    }
                )
        print(f'NUMBER OF FUNCTIONS WITHOUT EXPERIMENT CONFIG: {len(no_exp_config_found)}')
        for experiment_config, experiment_numbers_data in data_per_experiment.items():
            for experiment_number, exp_nr_data in experiment_numbers_data.items():
                print(f"Experiment config {experiment_config}; Experiment number {experiment_number}; Number of logged functions: {len(exp_nr_data)}")
        for experiment_config, experiment_number_data in data_per_experiment.items():
            json_file = glob.glob(
                f"/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/experiments_raw_data/results_{experiment_config}_intervals_{nr_intervals}.json"
            )
            experiment_data = None
            if json_file:
                with open(f"experiments_raw_data/results_{experiment_config}_intervals_{nr_intervals}.json", 'r') as file:
                    experiment_data = json.load(file)
            if experiment_data:
                experiment_data.update(experiment_number_data)
            else:
                experiment_data = experiment_number_data
            with open(f"experiments_raw_data/results_{experiment_config}_intervals_{nr_intervals}.json", 'w') as file:
                json.dump(experiment_data, file)

        # json_file = glob.glob(f"/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/experiments_raw_data/{EXPERIMENT_NAME}.json")
        # if json_file:
        #     resp = input(f"The file {EXPERIMENT_NAME} already exists. Do you want to override it? y/new file name")
        #     if resp == 'y':
        #         pass
        #     else:
        #         EXPERIMENT_NAME = resp
        #
        # with open(f'experiments_raw_data/{EXPERIMENT_NAME}.json', 'w') as file:
        #     json.dump(data, file)
    if REMOVE_CLOUDWATCH_LOGGING:
        log_groups = logs_client.describe_log_groups()
        print("Removing log groups")
        for log_group in log_groups['logGroups']:
            logs_client.delete_log_group(logGroupName=log_group['logGroupName'])

    if REMOVE_S3_LOGGING:
        print("Removing s3 logs")
        bucket.objects.filter(Prefix='lithops.jobs').delete()
        bucket.objects.filter(Prefix=LOGS_PREFIX).delete()

    if REMOVE_LOCAL_LOGGING:
        print("Removing local files")
        dirs = ['/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops.jobs',
                '/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/lithops_sorting_logs']
        for dir in dirs:
            for files in os.listdir(dir):
                path = os.path.join(dir, files)
                try:
                    shutil.rmtree(path)
                except OSError:
                    os.remove(path)
        # os.remove('/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/main_logging.txt')
    print("DONE PROCESSING LOGS")
    print(f"Number of report request id found: {nr_report_request_id_found}")
    print(f"Number of report request id found without init time: {nr_report_without_init_time_found}")
