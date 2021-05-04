from lithops import FunctionExecutor, Storage
from smart_open import open
import numpy as np
import io
from util import copyfileobj
import concurrent.futures

# PARTITION_ROWS = 40000000  # 4 GB
size_prefix = '100gb-1gb'
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


number_of_lambda_sessions_phase_1 = 10
number_of_lambda_sessions_phase_2 = 10


def iterate_file(filename):
    for line in open(filename, 'rb'):
        yield line


def thread_upload(key_name, nr_files, value, bucket, key):
    file_names = set()
    file_name = key_name.split('/')[1]
    new_file_name = f'{file_name}_{nr_files}'
    intermediate_files_temp = ''
    for k, v in subcategories.items():
        if (
                (not v['min'] and key <= v['max']) or
                (not v['max'] and v['min'] < key) or
                (v['min'] and v['max'] and v['min'] < key <= v['max'])
        ):
            intermediate_files_temp = f'{intermediate_files_dir}_{key}'
            break
    for second_key, second_value in value.items():
        intermediate_file_dir_temp = f'{intermediate_files_temp}/{key}_{second_key}'
        file_names.add(intermediate_file_dir_temp)
        with open(f's3://{bucket}/{intermediate_file_dir_temp}/{new_file_name}',
                  'wb') as intermediate_file:
            file_bytes = b"".join(second_value)
            intermediate_file.write(file_bytes)
    return file_names

# def thread_upload(key_name, nr_files, value, bucket, key):
#     file_names = set()
#     file_name = key_name.split('/')[1]
#     new_file_name = f'{file_name}_{nr_files}'
#     for second_key, second_value in value.items():
#         file_names.add(f'{key}_{second_key}')
#         with open(f's3://{bucket}/{intermediate_files_dir}/{key}_{second_key}/{new_file_name}',
#                   'wb') as intermediate_file:
#             file_bytes = b"".join(second_value)
#             intermediate_file.write(file_bytes)
#     return file_names


def write_partition(key_name, chars, nr_files, bucket):
    file_names = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures_upload = {executor.submit(thread_upload, key_name, nr_files, value, bucket, key): (key, value) for key, value in chars.items()}
        for future in concurrent.futures.as_completed(futures_upload):
            file_names |= future.result()

    return file_names


def determine_partition(chars, line):
    diff = line[1] - 32
    if chars.get(line[0]):
        for key, value in subcategories.items():
            if (
                    (not value['min'] and diff <= value['max']) or
                    (not value['max'] and value['min'] < diff) or
                    (value['min'] and value['max'] and value['min'] < diff <= value['max'])
            ):
                if chars[line[0]].get(key):
                    chars[line[0]][key].append(line)
                else:
                    chars[line[0]].update({key: [line]})
    else:
        for key, value in subcategories.items():
            if (
                    (not value['min'] and diff <= value['max']) or
                    (not value['max'] and value['min'] < diff) or
                    (value['min'] and value['max'] and value['min'] < diff <= value['max'])
            ):
                chars.update({line[0]: {key: [line]}})
    return chars


def determine_categories(key_name, storage):
    nr_files = 1
    count_rows = 0
    chars = {}
    unique_keys = set()
    for line in iterate_file(f's3://{storage.bucket}/{key_name}'):
        chars = determine_partition(chars, line)
        count_rows += 1

    # write remainings
    if chars:
        file_names = write_partition(key_name, chars, nr_files, storage.bucket)
        unique_keys = unique_keys.union(file_names)
    return unique_keys


def sort_category(category_key_name, storage):
    storage_client = Storage()
    # keys = storage_client.list_keys(storage.bucket, f'{intermediate_files_dir}/{category_key_name}/')
    keys = storage_client.list_keys(storage.bucket, f'{category_key_name}/')

    buf = io.BytesIO()

    for key_name in keys:
        with open(f's3://{storage.bucket}/{key_name}', 'rb',
                  transport_params=dict(client=storage.get_client())) as myfile:
            copyfileobj(myfile, buf)

    category_buffer = buf.getbuffer()
    del buf
    np_array = np.frombuffer(
        category_buffer, dtype=np.dtype([('sorted', 'bytes', 1), ('key', 'bytes', 9), ('value', 'bytes', 90)])
    )
    del category_buffer
    np_array = np.sort(np_array, order='key')
    file_name = category_key_name.split('/')[1]
    with open(f's3://{storage.bucket}/{output_prefix}/{file_name}', 'wb',
              transport_params=dict(client=storage.get_client())) as sorted_file:
        sorted_file.write(memoryview(np_array))


def sort():
    max_parallelism = None
    chars_set = set()

    with FunctionExecutor(runtime='bogdan/radix-sorting-container-100-mb-files') as fexec:
        bucket = fexec.config['lithops']['storage_bucket']
        storage_client = Storage()

        # PHASE 1
        current_keys_list = [{
            'key_name': key
        } for key in storage_client.list_keys(bucket, input_prefix + '/')]

        for i in range(number_of_lambda_sessions_phase_1):
            determine_categories_futures = fexec.map(determine_categories,
                                                     current_keys_list[
                                                                   i * (len(current_keys_list) // number_of_lambda_sessions_phase_1):
                                                                   (i+1) * (len(current_keys_list) // number_of_lambda_sessions_phase_1)])
            determine_categories_result = fexec.get_result(determine_categories_futures)
            for resulted_set in determine_categories_result:
                chars_set |= resulted_set

            print(f"============================ PHASE 1 ITERATION {i} ===============================")


        # Process remainings
        if len(current_keys_list) % number_of_lambda_sessions_phase_1 != 0:
            determine_categories_futures = fexec.map(determine_categories,
                                                     current_keys_list[
                                                     (i + 1) * (len(current_keys_list) // number_of_lambda_sessions_phase_1):])
            determine_categories_result = fexec.get_result(determine_categories_futures)
            for resulted_set in determine_categories_result:
                chars_set |= resulted_set

        print(f"!!!!!!!!!!!!!!!!!!!!!!!!!! END PHASE 1 ITERATION {i} !!!!!!!!!!!!!!!!!!!!!!!!!!")

        # PHASE 2
        fexec.config['serverless']['runtime_memory'] = 4800

        chars_list = list(chars_set)

        categories_keys_dict = [{
            'category_key_name': key_name
        } for key_name in chars_list]

        # categories_keys_dict = [{'category_key_name': '2_2g-input'}]
        for i in range(number_of_lambda_sessions_phase_2):
            sort_categories_futures = fexec.map(sort_category, categories_keys_dict[
                                                            i * (len(categories_keys_dict) // number_of_lambda_sessions_phase_2):
                                                            (i+1) * (len(categories_keys_dict) // number_of_lambda_sessions_phase_2)])
            sort_content_result = fexec.get_result(sort_categories_futures)
            print(f"============================ PHASE 2 ITERATION {i} ===============================")

        # Process remainings
        if len(categories_keys_dict) % number_of_lambda_sessions_phase_2 != 0:
            sort_categories_futures = fexec.map(sort_category, categories_keys_dict[
                                                            (i+1) * (len(categories_keys_dict) // number_of_lambda_sessions_phase_2):])
            sort_content_result = fexec.get_result(sort_categories_futures)

        print("DONE")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    sort()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
