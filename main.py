import json
import os

from lithops import FunctionExecutor, Storage
from smart_open import open
import numpy as np
import io
from util import copyfileobj
import boto3
from S3File import S3File
import logging

size_prefix = '1gb-1000files'
num_subcats = 2
nr_intervals = 256 * num_subcats
input_prefix = f'{size_prefix}-input'
output_prefix = f'{size_prefix}-output'
intermediate_files_dir = f'{input_prefix}-intermediate-files'
number_of_lambda_sessions_phase_1 = 1
number_of_lambda_sessions_phase_2 = 1
RUNTIME = 'bogdan/lambda-sort-2'

logger = logging.getLogger("lithops.handler")

EXECUTE_BOTH_PHASES = True


def upload_sorted_initial_file(bucket, key_name, record_arr):
    with open(f's3://{bucket}/{intermediate_files_dir}/{key_name}', 'wb') as sorted_file:
        sorted_file.write(memoryview(record_arr))


def read_file(s3, bucket, file_name, indexes):
    s3_object = s3.Object(bucket_name=bucket, key=f'{intermediate_files_dir}/{file_name}')
    s3file = S3File(s3_object, position=indexes[0] * 100)
    return s3file.read(
        size=(indexes[1] + 1) * 100 - indexes[0] * 100)


def determine_categories(key_name, storage, experiment_number, id):
    activation_id = os.environ['__LITHOPS_ACTIVATION_ID']
    print(f"[WORKER {activation_id}] Start 'determine categories' phase.")

    buf = io.BytesIO()
    locations = {}
    print(f'worker_id={activation_id};function_id={id};experiment_config={size_prefix};experiment_number={experiment_number};')
    print(f"[WORKER {activation_id}] Start downloading initial file.")

    with open(f's3://{storage.bucket}/{key_name}', 'rb',
              transport_params=dict(client=storage.get_client())) as myfile:
        copyfileobj(myfile, buf)

    print(f"[WORKER {activation_id}] Finish downloading initial file.")

    category_buffer = buf.getbuffer()
    del buf
    record_arr = np.frombuffer(category_buffer, dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    del category_buffer

    print(f"[WORKER {activation_id}] Start first sorting.")

    record_arr = np.sort(record_arr, order='key')

    print(f"[WORKER {activation_id}] Finish first sorting.")

    sorted_file_name = key_name.split('/')[1]

    print(f"[WORKER {activation_id}] Start uploading first sorted file.")

    upload_sorted_initial_file(storage.bucket, sorted_file_name, record_arr)

    print(f"[WORKER {activation_id}] Finish uploading first sorted file.")

    first_char = None
    start_index = 0
    current_file_number_per_first_char = 1
    diff = 256 // num_subcats
    lower_margin = 0
    upper_margin = diff
    new_file_name = ''
    nr_elements = 0
    for nr_elements, rec in enumerate(record_arr):
        key_array = bytearray(rec[0])
        if first_char is None:
            first_char = key_array[0]
        new_file_name = f'{first_char}_{current_file_number_per_first_char}'

        if key_array[0] != first_char or (key_array[1] < lower_margin or key_array[1] > upper_margin):
            locations[new_file_name] = {
                'start_index': start_index,
                'end_index': nr_elements - 1,
                'file_name': sorted_file_name
            }

            if key_array[0] != first_char:
                current_file_number_per_first_char = 1
                start_index = nr_elements
                lower_margin = 0
                upper_margin = diff
                first_char = key_array[0]
            else:
                current_file_number_per_first_char += 1
                lower_margin = upper_margin + 1
                upper_margin = lower_margin + diff
                start_index = nr_elements

    locations[new_file_name] = {
        'start_index': start_index,
        'end_index': nr_elements,
        'file_name': sorted_file_name
    }
    print(f"[WORKER {activation_id}] Finish 'determine categories' phase.")
    return locations


def sort_category(category_key_name, storage, experiment_number, id):
    activation_id = os.environ['__LITHOPS_ACTIVATION_ID']
    print(f"[WORKER {activation_id}] Start 'sort_category' phase")
    print(f'worker_id={activation_id};function_id={id};experiment_config={size_prefix};experiment_number={experiment_number};')
    s3 = boto3.resource("s3")
    for category_partition_name, files in category_key_name.items():
        buf = io.BytesIO()
        # with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        #     futures = {executor.submit(read_file, s3, storage.bucket, file_name, indexes) for file_name, indexes in files.items()}
        #     for future in concurrent.futures.as_completed(futures):
        #         buf.write(future.result())
        print(f"[WORKER {activation_id}] Start download intervals.")
        for file_name, indexes in files.items():
            print(f"[WORKER {activation_id}] Start download interval file {file_name}.")
            s3_object = s3.Object(bucket_name=storage.bucket, key=f'{intermediate_files_dir}/{file_name}')
            s3file = S3File(s3_object, position=indexes[0] * 100)
            file_content = s3file.read(
                size=(indexes[1] + 1) * 100 - indexes[0] * 100)
            print(f"[WORKER {activation_id}] Finish download interval file {file_name}.")
            buf.write(file_content)
        print(f"[WORKER {activation_id}] Finish download intervals.")
        category_buffer = buf.getbuffer()
        del buf
        np_array = np.frombuffer(
            category_buffer, dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
        )
        del category_buffer
        print(f"[WORKER {activation_id}] Start sort final file")
        np_array = np.sort(np_array, order='key')
        print(f"[WORKER {activation_id}] Finish sort final file")
        print(f"[WORKER {activation_id}] Start write final file")

        # TODO: UPDATE BUCKET NAME HERE FOR UPLOADING FINAL FILES
        with open(f's3://output-sorting-experiments/{output_prefix}/{category_partition_name}', 'wb',
                  transport_params=dict(client=storage.get_client())) as sorted_file:
            sorted_file.write(memoryview(np_array))
        print(f"[WORKER {activation_id}] Finish write final file")
        print(f"[WORKER {activation_id}] Finish 'sort_category' phase")


def sort(EXPERIMENT_NUMBER):
    stats = {}
    determine_categories_result = []

    with FunctionExecutor(runtime=RUNTIME) as fexec:
        bucket = fexec.config['lithops']['storage_bucket']
        storage_client = Storage()

        # PHASE 1
        current_keys_list = [{
            'key_name': key
        } for key in storage_client.list_keys(bucket, input_prefix + '/')]

        print("Start first phase (determine categories)")

        for i in range(number_of_lambda_sessions_phase_1):
            determine_categories_futures = fexec.map(determine_categories,
                                                     current_keys_list[
                                                     i * (len(current_keys_list) // number_of_lambda_sessions_phase_1):
                                                     (i + 1) * (len(
                                                         current_keys_list) // number_of_lambda_sessions_phase_1)],
                                                     extra_args={'experiment_number': EXPERIMENT_NUMBER})
            determine_categories_result += fexec.get_result(determine_categories_futures)

        # Save stats to file
        for future in fexec.futures:
            stats.update({future.activation_id: future.stats})
        if not os.path.isdir(f'plots_{size_prefix}_{nr_intervals}'):
            os.mkdir(f'plots_{size_prefix}_{nr_intervals}')
        fexec.plot(dst=f'plots_{size_prefix}_{nr_intervals}/main_phase1_{size_prefix}_intervals{nr_intervals}_ExpNr_{EXPERIMENT_NUMBER}')
        # PROCESS REMAININGS
        # if len(current_keys_list) % number_of_lambda_sessions_phase_1 != 0:
        #     determine_categories_futures = fexec.map(determine_categories,
        #                                              current_keys_list[
        #                                              (i + 1) * (len(
        #                                                  current_keys_list) // number_of_lambda_sessions_phase_1):])
        #     determine_categories_result += fexec.get_result(determine_categories_futures)

    with FunctionExecutor(runtime=RUNTIME) as fexec:
        if EXECUTE_BOTH_PHASES:
            formatted = {}

            for file in determine_categories_result:
                for category_partition_name, file_with_indexes in file.items():
                    if not formatted.get(category_partition_name):
                        formatted[category_partition_name] = {
                            file_with_indexes['file_name']:
                                [file_with_indexes['start_index'], file_with_indexes['end_index']]
                        }
                    else:
                        formatted[category_partition_name].update({
                            file_with_indexes['file_name']:
                                [file_with_indexes['start_index'], file_with_indexes['end_index']]

                        })
            fexec.config['aws_lambda']['runtime_memory'] = 4800
            formatted_list = [{'category_key_name': {key: value}} for key, value in formatted.items()]
            with open("start_phase_2.json", 'w') as file:
                json.dump(formatted_list, file)
            print("================== START PHASE 2 ======================")

            nr_phases = 0
            for nr_phases in range(number_of_lambda_sessions_phase_2):
                sort_categories_futures = fexec.map(
                    sort_category,
                    formatted_list[
                        nr_phases * (len(formatted_list) // number_of_lambda_sessions_phase_2):
                        (nr_phases + 1) * (len(formatted_list) // number_of_lambda_sessions_phase_2)],
                    extra_args={'experiment_number': EXPERIMENT_NUMBER}
                )
                sort_content_result = fexec.get_result(sort_categories_futures)

            for future in fexec.futures:
                stats.update({future.activation_id: future.stats})
            fexec.plot(dst=f'plots_{size_prefix}_{nr_intervals}/main_phase2_{size_prefix}_intervals{nr_intervals}_ExpNr_{EXPERIMENT_NUMBER}')

            # Process remainings
            # if len(formatted_list) % number_of_lambda_sessions_phase_2 != 0:
            #     sort_categories_futures = fexec.map(sort_category, formatted_list[
            #                                                        (nr_phases + 1) * (len(
            #                                                            formatted_list) // number_of_lambda_sessions_phase_2):])
            #     sort_content_result = fexec.get_result(sort_categories_futures)

    with open(f"stats/stats_logging_{size_prefix}_ExpNr_{EXPERIMENT_NUMBER}_intervals_{nr_intervals}.json", 'w') as stats_file:
        json.dump(stats, stats_file)
    print("DONE")


if __name__ == '__main__':
    for i in range(1, 11):
        print(f'Running EXPERIMENT_NUMBER={i} for prefix {size_prefix}')
        sort(i)
        print(f'Finished EXPERIMENT_NUMBER={i} for prefix {size_prefix}')

