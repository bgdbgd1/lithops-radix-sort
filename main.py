from lithops import FunctionExecutor, Storage
from smart_open import open
import numpy as np
import io
from util import copyfileobj
import concurrent.futures
import boto3
from S3File import S3File

# PARTITION_ROWS = 40000000  # 4 GB
size_prefix = '20mb-binary'
input_prefix = f'{size_prefix}-input'
output_prefix = f'{size_prefix}-output'
intermediate_files_dir = f'{input_prefix}-intermediate-files'

number_of_subcategories = 5

nr_subcats = number_of_subcategories
if 95 % number_of_subcategories != 0:
    nr_subcats = number_of_subcategories + 1

subcategories = {}

for i in range(nr_subcats):
    if i == 0:
        subcategories.update(
            {
                i: {
                    'min': None,
                    'max': (i + 1) * (95 // number_of_subcategories)
                }
            }
        )
    elif i == nr_subcats - 1:
        subcategories.update({
            i: {
                'min': i * (95 // number_of_subcategories),
                'max': None
            }
        })
    else:
        subcategories.update({i: {
            'min': i * (95 // number_of_subcategories),
            'max': (i + 1) * (95 // number_of_subcategories)
        }})

number_of_lambda_sessions_phase_1 = 1
number_of_lambda_sessions_phase_2 = 3


def iterate_file(filename):
    for line in open(filename, 'rb'):
        yield line


def upload_sorted_initial_file(bucket, key_name, record_arr):
    with open(f's3://{bucket}/{intermediate_files_dir}/{key_name}', 'wb') as sorted_file:
        sorted_file.write(memoryview(record_arr))


def determine_categories(key_name, storage):
    buf = io.BytesIO()
    locations = {}

    with open(f's3://{storage.bucket}/{key_name}', 'rb',
              transport_params=dict(client=storage.get_client())) as myfile:
        copyfileobj(myfile, buf)

    category_buffer = buf.getbuffer()
    del buf
    record_arr = np.frombuffer(category_buffer, dtype=np.dtype([('key', 'V2'), ('rest', 'V98')]))
    del category_buffer
    record_arr = np.sort(record_arr, order='key')
    sorted_file_name = key_name.split('/')[1]

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(upload_sorted_initial_file, storage.bucket, sorted_file_name, record_arr)

    first_char = None
    start_index = 0
    current_file_number_per_first_char = 1
    num_subcats = 2
    diff = 265 // num_subcats
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
            current_file_number_per_first_char += 1
            lower_margin = upper_margin + 1
            upper_margin = lower_margin + diff
            start_index = nr_elements

            if key_array[0] != first_char:
                current_file_number_per_first_char = 1
                start_index = nr_elements
                lower_margin = 0
                upper_margin = diff
                first_char = key_array[0]

    locations[new_file_name] = {
        'start_index': start_index,
        'end_index': nr_elements,
        'file_name': sorted_file_name
    }

    return locations


def sort_category(category_key_name, storage):
    s3 = boto3.resource("s3")
    for category_partition_name, files in category_key_name.items():
        buf = io.BytesIO()
        for file_name, indexes in files.items():
            s3_object = s3.Object(bucket_name=storage.bucket, key=f'{intermediate_files_dir}/{file_name}')
            s3file = S3File(s3_object, position=indexes['start_index'] * 100)
            file_content = s3file.read(
                size=(indexes['end_index'] + 1) * 100 - indexes['start_index'] * 100)
            buf.write(file_content)

        category_buffer = buf.getbuffer()
        del buf
        np_array = np.frombuffer(
            category_buffer, dtype=np.dtype([('sorted', 'V1'), ('key', 'V9'), ('value', 'V90')])
        )
        del category_buffer
        np_array = np.sort(np_array, order='key')
        with open(f's3://{storage.bucket}/{output_prefix}/{category_partition_name}', 'wb',
                  transport_params=dict(client=storage.get_client())) as sorted_file:
            sorted_file.write(memoryview(np_array))


def sort():
    with FunctionExecutor(runtime='bogdan/radix-sorting-container-100-mb-files') as fexec:
        bucket = fexec.config['lithops']['storage_bucket']
        storage_client = Storage()

        # PHASE 1
        current_keys_list = [{
            'key_name': key
        } for key in storage_client.list_keys(bucket, input_prefix + '/')]

        formatted = {}
        determine_categories_result = []
        for i in range(number_of_lambda_sessions_phase_1):
            determine_categories_futures = fexec.map(determine_categories,
                                                     current_keys_list[
                                                     i * (len(current_keys_list) // number_of_lambda_sessions_phase_1):
                                                     (i + 1) * (len(
                                                         current_keys_list) // number_of_lambda_sessions_phase_1)])
            determine_categories_result += fexec.get_result(determine_categories_futures)

        # PROCESS REMAININGS
        if len(current_keys_list) % number_of_lambda_sessions_phase_1 != 0:
            determine_categories_futures = fexec.map(determine_categories,
                                                     current_keys_list[
                                                     (i + 1) * (len(
                                                         current_keys_list) // number_of_lambda_sessions_phase_1):])
            determine_categories_result += fexec.get_result(determine_categories_futures)

        for file in determine_categories_result:
            for category_partition_name, file_with_indexes in file.items():
                if not formatted.get(category_partition_name):
                    formatted[category_partition_name] = {
                        file_with_indexes['file_name']: {
                            'start_index': file_with_indexes['start_index'],
                            'end_index': file_with_indexes['end_index']
                        }
                    }
                else:
                    formatted[category_partition_name].update({
                        file_with_indexes['file_name']: {
                            'start_index': file_with_indexes['start_index'],
                            'end_index': file_with_indexes['end_index']
                        }
                    })
        # fexec.config['serverless']['runtime_memory'] = 4800
        formatted_list = [{'category_key_name': {key: value}} for key, value in formatted.items()]
        print(formatted_list)
        print("================== START PHASE 2 ======================")
        nr_phases = 0
        for nr_phases in range(number_of_lambda_sessions_phase_2):
            sort_categories_futures = fexec.map(sort_category, formatted_list[
                                                               nr_phases * (len(
                                                                   formatted_list) // number_of_lambda_sessions_phase_2):
                                                               (nr_phases + 1) * (len(
                                                                   formatted_list) // number_of_lambda_sessions_phase_2)])
            sort_content_result = fexec.get_result(sort_categories_futures)

        # Process remainings
        if len(formatted_list) % number_of_lambda_sessions_phase_2 != 0:
            sort_categories_futures = fexec.map(sort_category, formatted_list[
                                                               (nr_phases + 1) * (len(
                                                                   formatted_list) // number_of_lambda_sessions_phase_2):])
            sort_content_result = fexec.get_result(sort_categories_futures)
        print("DONE")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sort()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
