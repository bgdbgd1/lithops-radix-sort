import os
from datetime import datetime, timedelta

import pylab
import pandas as pd
import matplotlib.pyplot as plt
import json
import seaborn as sns
import numpy as np
import matplotlib.patches as mpatches
import time


def get_time_difference(sorted_array, lower_limit, upper_limit, field_name=None):
    for item in sorted_array:
        dif = datetime.fromtimestamp(item[upper_limit]) - datetime.fromtimestamp(item[lower_limit])
        dif_sec = dif.total_seconds()
        item[field_name] = dif_sec
    return sorted_array

    #
    # difference_array = []
    # start_times = [item[lower_limit] for item in sorted_array]
    # finish_times = [item[upper_limit] for item in sorted_array]
    # for i in range(0, len(start_times)):
    #     dif = datetime.fromtimestamp(finish_times[i]) - datetime.fromtimestamp(start_times[i])
    #     dif_sec = dif.total_seconds()
    #     difference_array.append(dif_sec)
    #
    # return difference_array


def create_boxplot(data, savepath):
    fig = plt.figure(figsize=(10, 7))

    # Creating plot
    plt.boxplot(data)
    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')
    plt.clf()


def create_barchart(ids, values, title, x_label, y_label, savepath, enable_legend=False):
    plt.margins(x=0)
    for val in values:
        plt.bar(ids, val, width=1.0)
    plt.title(title, fontsize=10)
    plt.xlabel(x_label, fontsize=10)
    plt.ylabel(y_label, fontsize=10)
    plt.grid(True)
    if enable_legend:
        legend = ['Execution Duration', 'Init Duration']
        plt.legend(legend, loc='upper right')
    # plt.show()
    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')

    plt.clf()


def create_timeline_stage_1(stats, dst):
    host_job_create_tstamp = min([cm['host_job_create_tstamp'] for cm in stats])

    stats_df = pd.DataFrame(stats)
    total_calls = len(stats_df)

    palette = sns.color_palette("deep", 15)

    fig = pylab.figure(figsize=(10, 6))
    ax = fig.add_subplot(1, 1, 1)

    y = np.arange(total_calls)
    point_size = 10

    fields = [
        ('host submit', stats_df.host_submit_tstamp - host_job_create_tstamp),
        ('call start', stats_df.worker_start_tstamp - host_job_create_tstamp),
        ('start download initial file',
         stats_df.start_downloading_initial_file_timestamp - host_job_create_tstamp),
        ('finish download initial file',
         stats_df.finish_downloading_initial_file_timestamp - host_job_create_tstamp),
        # ('start first sorting', stats_df.start_first_sorting_timestamp - host_job_create_tstamp),
        # ('finish first sorting', stats_df.finish_first_sorting_timestamp - host_job_create_tstamp),
        ('start uploading sorted file',
         stats_df.start_uploading_first_sorted_file_timestamp - host_job_create_tstamp),
        ('finish uploading sorted file',
         stats_df.finish_uploading_first_sorted_file_timestamp - host_job_create_tstamp),
        # ('finish determine categories phase', stats_df.finish_determine_categories_phase_timestamp - host_job_create_tstamp),
        ('call done', stats_df.worker_end_tstamp - host_job_create_tstamp),
        # ('status fetched', stats_df.host_status_done_tstamp - host_job_create_tstamp),
    ]

    # if 'host_result_done_tstamp' in stats_df:
    #     fields.append(('results fetched', stats_df.host_result_done_tstamp - host_job_create_tstamp))
    patches = []
    for f_i, (field_name, val) in enumerate(fields):
        ax.scatter(val, y, c=[palette[f_i]], edgecolor='none', s=point_size, alpha=0.8)
        patches.append(mpatches.Patch(color=palette[f_i], label=field_name))

    ax.set_xlabel('Execution Time (sec)')
    ax.set_ylabel('Function Call')

    legend = pylab.legend(handles=patches, loc='upper right', frameon=True)
    legend.get_frame().set_facecolor('#FFFFFF')

    yplot_step = int(np.max([1, total_calls / 20]))
    y_ticks = np.arange(total_calls // yplot_step + 2) * yplot_step
    ax.set_yticks(y_ticks)
    ax.set_ylim(-0.02 * total_calls, total_calls * 1.02)
    for y in y_ticks:
        ax.axhline(y, c='k', alpha=0.1, linewidth=1)

    if 'host_result_done_tstamp' in stats_df:
        max_seconds = np.max(stats_df.host_result_done_tstamp - host_job_create_tstamp) * 1.25
    elif 'host_status_done_tstamp' in stats_df:
        max_seconds = np.max(stats_df.host_status_done_tstamp - host_job_create_tstamp) * 1.25
    else:
        max_seconds = np.max(stats_df.end_tstamp - host_job_create_tstamp) * 1.25
    xplot_step = max(int(max_seconds / 8), 1)
    x_ticks = np.arange(max_seconds // xplot_step + 2) * xplot_step
    ax.set_xlim(0, max_seconds)

    ax.set_xticks(x_ticks)
    for x in x_ticks:
        ax.axvline(x, c='k', alpha=0.2, linewidth=0.8)

    ax.grid(False)
    fig.tight_layout()

    if dst is None:
        os.makedirs('plots', exist_ok=True)
        dst = os.path.join(os.getcwd(), 'plots', '{}_{}'.format(int(time.time()), 'timeline.png'))
    else:
        dst = os.path.expanduser(dst) if '~' in dst else dst
        dst = '{}_{}'.format(os.path.realpath(dst), 'timeline.png')

    fig.savefig(dst)


def create_timeline_stage_2(stats, dst):
    host_job_create_tstamp = min([cm['host_job_create_tstamp'] for cm in stats])

    stats_df = pd.DataFrame(stats)
    total_calls = len(stats_df)

    palette = sns.color_palette("deep", 15)

    fig = pylab.figure(figsize=(10, 6))
    ax = fig.add_subplot(1, 1, 1)

    y = np.arange(total_calls)
    point_size = 10

    fields = [('Host submit', stats_df.host_submit_tstamp - host_job_create_tstamp),
              ('Call start', stats_df.worker_start_tstamp - host_job_create_tstamp),
              ('Start download partitions', stats_df.start_download_intervals_timestamp - host_job_create_tstamp),
              ('Finish download partitions', stats_df.finish_download_intervals_timestamp - host_job_create_tstamp),
              # ('Start sort category', stats_df.start_sort_final_file_timestamp - host_job_create_tstamp),
              # ('Finish sort category', stats_df.finish_sort_final_file_timestamp - host_job_create_tstamp),
              # ('Start upload category to S3', stats_df.start_write_final_file_timestamp - host_job_create_tstamp),
              # ('Finish upload category to S3', stats_df.finish_write_final_file_timestamp - host_job_create_tstamp),
              ('Finish sort category phase', stats_df.finish_sort_category_phase_timestamp - host_job_create_tstamp),
              ('Call done', stats_df.worker_end_tstamp - host_job_create_tstamp),
              # ('status fetched', stats_df.host_status_done_tstamp - host_job_create_tstamp)
              ]

    # if 'host_result_done_tstamp' in stats_df:
    #     fields.append(('results fetched', stats_df.host_result_done_tstamp - host_job_create_tstamp))

    patches = []
    for f_i, (field_name, val) in enumerate(fields):
        ax.scatter(val, y, c=[palette[f_i]], edgecolor='none', s=point_size, alpha=0.8)
        patches.append(mpatches.Patch(color=palette[f_i], label=field_name))

    ax.set_xlabel('Execution Time (sec)')
    ax.set_ylabel('Function Call')

    legend = pylab.legend(handles=patches, loc='upper right', frameon=True)
    legend.get_frame().set_facecolor('#FFFFFF')

    yplot_step = int(np.max([1, total_calls / 20]))
    y_ticks = np.arange(total_calls // yplot_step + 2) * yplot_step
    ax.set_yticks(y_ticks)
    ax.set_ylim(-0.02 * total_calls, total_calls * 1.02)
    for y in y_ticks:
        ax.axhline(y, c='k', alpha=0.1, linewidth=1)

    if 'host_result_done_tstamp' in stats_df:
        max_seconds = np.max(stats_df.host_result_done_tstamp - host_job_create_tstamp) * 1.25
    elif 'host_status_done_tstamp' in stats_df:
        max_seconds = np.max(stats_df.host_status_done_tstamp - host_job_create_tstamp) * 1.25
    else:
        max_seconds = np.max(stats_df.end_tstamp - host_job_create_tstamp) * 1.25
    xplot_step = max(int(max_seconds / 8), 1)
    x_ticks = np.arange(max_seconds // xplot_step + 2) * xplot_step
    ax.set_xlim(0, max_seconds)

    ax.set_xticks(x_ticks)
    for x in x_ticks:
        ax.axvline(x, c='k', alpha=0.2, linewidth=0.8)

    ax.grid(False)
    fig.tight_layout()

    if dst is None:
        os.makedirs('plots', exist_ok=True)
        dst = os.path.join(os.getcwd(), 'plots', '{}_{}'.format(int(time.time()), 'timeline.png'))
    else:
        dst = os.path.expanduser(dst) if '~' in dst else dst
        dst = '{}_{}'.format(os.path.realpath(dst), 'timeline.png')

    fig.savefig(dst)


def convert_localdt_to_utc(val):
    val_dt = datetime.fromtimestamp(val)
    return datetime.timestamp(val_dt - timedelta(hours=2))


def create_plots(EXPERIMENT_CONFIG, nr_intervals, experiment_number):
    stage_1_list = []
    stage_2_list = []
    stage_1_data = {}
    stage_2_data = {}
    no_function_id = []
    init_duration_zero = []
    experiment_data = None
    timestamps_to_transform = [
        "host_job_create_tstamp",
        "host_submit_tstamp",
        "worker_start_tstamp",
        "worker_end_tstamp",
        "host_status_done_tstamp",
        "host_result_done_tstamp"
    ]
    with open(f"experiments_raw_data/results_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}.json", "r") as read_file:
        experiments_data = json.load(read_file)
        experiment_data = experiments_data[str(experiment_number)]
    for key, val in experiment_data.items():
        for field in timestamps_to_transform:
            if val.get(field):
                value_field = val[field]
                v = convert_localdt_to_utc(val[field])
                val[field] = v
        if val.get('init_duration') == 0:
            init_duration_zero.append(val)
        if val.get('function_id') is None:
            no_function_id.append(val)
            continue
        elif val.get('start_determine_categories_phase'):
            stage_1_list.append(val)
            stage_1_data.update({key: val})
        elif val.get('start_sort_category_phase'):
            stage_2_list.append(val)
            stage_2_data.update({key: val})

    if len(no_function_id):
        print(f"NO FUNCTION ID LOGS FOUND no_function_id={len(no_function_id)}! MIGHT NEED TO REDOWNLOAD THE LOGS!")
        return f"Functions without ids: {no_function_id}"
    # with open(f'experiments_raw_data/stage1_elem.json', 'w') as file:
    #     json.dump(stage_1_list[0], file)
    # with open(f'experiments_raw_data/stage2_elem.json', 'w') as file:
    #     json.dump(stage_2_list[0], file)

    sorted_stage1_list = sorted(stage_1_list, key=lambda k: k['function_id'])
    sorted_stage2_list = sorted(stage_2_list, key=lambda k: k['function_id'])

    # print("Creating timeline stage 1")
    create_timeline_stage_1(sorted_stage1_list,
                            f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/stage1_{EXPERIMENT_CONFIG}_{experiment_number}')
    plt.clf()
    # exit()

    # print("Creating timeline stage 2")
    create_timeline_stage_2(sorted_stage2_list,
                            f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/stage2_{EXPERIMENT_CONFIG}_{experiment_number}')
    plt.clf()

    # exit()

    # print("Processing data for barcharts")
    ids_stage1 = [item['function_id'] for item in sorted_stage1_list]
    ids_stage2 = [item['function_id'] for item in sorted_stage2_list]
    ids_with_init_duration_stage1 = [item['function_id'] for item in sorted_stage1_list]
    ids_with_init_duration_stage2 = [item['function_id'] for item in sorted_stage2_list]

    billed_time_stage_1 = [item['billed_duration_float'] for item in sorted_stage1_list]
    billed_time_stage_2 = [item['billed_duration_float'] for item in sorted_stage2_list]

    execution_time_stage_1 = [item['execution_duration_float'] for item in sorted_stage1_list]
    execution_time_stage_2 = [item['execution_duration_float'] for item in sorted_stage2_list]

    init_time_stage_1 = []
    for i in range(0, len(sorted_stage1_list)):
        if sorted_stage1_list[i]['init_duration'] != 0:
            init_time_stage_1.append(sorted_stage1_list[i]['init_duration_float'])
        else:
            init_time_stage_1.append(billed_time_stage_1[i] - execution_time_stage_1[i])

    init_time_stage_2 = []
    for i in range(0, len(sorted_stage2_list)):
        if sorted_stage2_list[i]['init_duration'] != 0:
            init_time_stage_2.append(sorted_stage2_list[i]['init_duration_float'])
        else:
            init_time_stage_2.append(billed_time_stage_2[i] - execution_time_stage_2[i])

    sorted_stage1_list = get_time_difference(
        sorted_stage1_list,
        'start_downloading_initial_file_timestamp',
        'finish_downloading_initial_file_timestamp',
        'download_initial_file_duration'
    )

    sorted_stage1_list = get_time_difference(
        sorted_stage1_list,
        'start_uploading_first_sorted_file_timestamp',
        'finish_uploading_first_sorted_file_timestamp',
        'upload_initial_file_duration',
    )

    sorted_stage1_list = get_time_difference(
        sorted_stage1_list,
        'finish_uploading_first_sorted_file_timestamp',
        'finish_determine_categories_phase_timestamp',
        'determine_intervals_duration'
    )

    determine_intervals_duration_stage_1 = [item['determine_intervals_duration'] for item in sorted_stage1_list]

    sorted_stage1_list = get_time_difference(
        sorted_stage1_list,
        'start_first_sorting_timestamp',
        'finish_first_sorting_timestamp',
        'first_sorting_duration'
    )

    first_sorting_duration_stage_1 = [item['first_sorting_duration'] for item in sorted_stage1_list]

    sorted_stage2_list = get_time_difference(
        sorted_stage2_list,
        'start_download_interval_file_0_timestamp',
        'finish_download_interval_file_0_timestamp',
        'download_interval_file_0_duration'
    )
    download_interval_file_0_stage_2 = [item['download_interval_file_0_duration'] for item in sorted_stage2_list]


    # download_interval_file_180_stage_2 = get_time_difference(
    #     sorted_stage2_list,
    #     'start_download_interval_file_180_timestamp',
    #     'finish_download_interval_file_180_timestamp'
    # )
    #
    # download_last_interval_file_stage_2 = get_time_difference(
    #     sorted_stage2_list,
    #     'start_download_interval_file_256_timestamp',
    #     'finish_download_interval_file_256_timestamp'
    # )

    download_intervals_function_180_stage_2 = []
    function_180 = sorted_stage2_list[180]
    for i in range(0, 10):
        start_key = f'start_download_interval_file_{i}_timestamp'
        finish_key = f'finish_download_interval_file_{i}_timestamp'
        dif = datetime.fromtimestamp(function_180[finish_key]) - datetime.fromtimestamp(function_180[start_key])
        dif_sec = dif.total_seconds()
        download_intervals_function_180_stage_2.append(dif_sec)

    sorted_stage2_list = get_time_difference(
        sorted_stage2_list,
        'start_sort_final_file_timestamp',
        'finish_sort_final_file_timestamp',
        'sort_final_file_duration'
    )
    sort_final_file_stage_2 = [item['sort_final_file_duration'] for item in sorted_stage2_list]

    sorted_stage2_list = get_time_difference(
        sorted_stage2_list,
        'start_write_final_file_timestamp',
        'finish_write_final_file_timestamp',
        'write_final_file_duration'
    )
    upload_final_file_stage_2 = [item['write_final_file_duration'] for item in sorted_stage2_list]


    upload_sorted_file_stage_1 = [item['upload_initial_file_duration'] for item in sorted_stage1_list]
    download_init_file_times_stage_1 = [item['download_initial_file_duration'] for item in sorted_stage1_list]
    determine_intervals_stage_1 = [item['determine_intervals_duration'] for item in sorted_stage1_list]
    # Get max execution time, download init and upload init time
    function_max_exec_time = 0
    function_max_download_time = 0
    function_max_upload_time = 0
    max_exec_time = 0.0
    max_down_time = 0.0
    max_up_time = 0.0
    down_time_for_max_exec_time = 0.0
    up_time_for_max_exec_time = 0.0
    exec_time_for_max_down_time = 0.0
    up_time_for_max_down_time = 0.0
    exec_time_for_max_up_time = 0.0
    down_time_for_max_up_time = 0.0
    interval_for_max_exec_time = 0.0
    interval_for_max_up_time = 0.0
    interval_for_max_down_time = 0.0
    sorting_for_max_exec_time = 0.0
    sorting_for_max_up_time = 0.0
    sorting_for_max_down_time = 0.0

    for i in range(0, len(execution_time_stage_1)):
        if max_up_time < upload_sorted_file_stage_1[i]:
            max_up_time = upload_sorted_file_stage_1[i]
            function_max_upload_time = i
            exec_time_for_max_up_time = execution_time_stage_1[i]
            down_time_for_max_up_time = download_init_file_times_stage_1[i]
            interval_for_max_up_time = determine_intervals_stage_1[i]
            sorting_for_max_up_time = first_sorting_duration_stage_1[i]

        if max_down_time < download_init_file_times_stage_1[i]:
            max_down_time = download_init_file_times_stage_1[i]
            function_max_download_time = i
            exec_time_for_max_down_time = execution_time_stage_1[i]
            up_time_for_max_down_time = upload_sorted_file_stage_1[i]
            interval_for_max_down_time = determine_intervals_stage_1[i]
            sorting_for_max_down_time = first_sorting_duration_stage_1[i]

        if max_exec_time < execution_time_stage_1[i]:
            max_exec_time = execution_time_stage_1[i]
            function_max_exec_time = i
            down_time_for_max_exec_time = download_init_file_times_stage_1[i]
            up_time_for_max_exec_time = upload_sorted_file_stage_1[i]
            interval_for_max_exec_time = determine_intervals_stage_1[i]
            sorting_for_max_exec_time = first_sorting_duration_stage_1[i]


    print(
        f"MAX EXEC TIME {max_exec_time} on function {function_max_exec_time} with download time {down_time_for_max_exec_time} and upload time {up_time_for_max_exec_time} and interval time {interval_for_max_exec_time} and sorting time {sorting_for_max_exec_time}")
    print(
        f'MAX DOWN TIME {max_down_time} on function {function_max_download_time} with upload time {up_time_for_max_down_time} and exec time {exec_time_for_max_down_time} and interval time {interval_for_max_down_time} and sorting time {sorting_for_max_down_time}')
    print(
        f'MAX UP TIME {max_up_time} on function {function_max_upload_time} with download time {down_time_for_max_up_time} and exec time {exec_time_for_max_up_time} and interval time {interval_for_max_up_time} and sorting time {sorting_for_max_up_time}')
    # exit()

    # Overlap timestamps

    intervals_download_init_file = {}
    intervals_upload_sorted_file_stage_1 = {}
    for function_record in sorted_stage1_list:
        intervals_download_init_file.update(
            {
                (
                    function_record['start_downloading_initial_file_timestamp'],
                    function_record['finish_downloading_initial_file_timestamp']
                ): []
            }
        )
        intervals_upload_sorted_file_stage_1.update(
            {
                (
                    function_record['start_uploading_first_sorted_file_timestamp'],
                    function_record['finish_uploading_first_sorted_file_timestamp']
                ): []
            }
        )

        for interval, functions in intervals_download_init_file.items():
            if len(functions) > 0:
                if interval[0] <= function_record['start_downloading_initial_file_timestamp'] < interval[1]:
                    functions.append(
                        {
                            "start_downloading_initial_file_timestamp": function_record[
                                'start_downloading_initial_file_timestamp'],
                            "finish_downloading_initial_file_timestamp": function_record[
                                'finish_downloading_initial_file_timestamp'],
                            "function_id": function_record['function_id']
                        }
                    )
            else:
                functions.append(
                    {
                        "start_downloading_initial_file_timestamp": function_record[
                            'start_downloading_initial_file_timestamp'],
                        "finish_downloading_initial_file_timestamp": function_record[
                            'finish_downloading_initial_file_timestamp'],
                        "function_id": function_record['function_id']
                    }
                )

        for interval, functions in intervals_upload_sorted_file_stage_1.items():
            if len(functions) > 0:
                if interval[0] <= function_record['start_uploading_first_sorted_file_timestamp'] < interval[1]:
                    functions.append(
                        {
                            "start_uploading_first_sorted_file_timestamp": function_record[
                                'start_uploading_first_sorted_file_timestamp'],
                            "finish_uploading_first_sorted_file_timestamp": function_record[
                                'finish_uploading_first_sorted_file_timestamp'],
                            "function_id": function_record['function_id']
                        }
                    )
            else:
                functions.append(
                    {
                        "start_uploading_first_sorted_file_timestamp": function_record[
                            'start_uploading_first_sorted_file_timestamp'],
                        "finish_uploading_first_sorted_file_timestamp": function_record[
                            'finish_uploading_first_sorted_file_timestamp'],
                        "function_id": function_record['function_id']
                    }
                )

    # Determine the biggest interval from which the function with biggest upload/download time is part of
    biggest_interval_max_up_time = None
    biggest_interval_max_down_time = None
    entry_interval_max_up_time = None
    entry_interval_max_down_time = None
    entry_interval_max_down_time_nr_functions = 0
    entry_interval_max_up_time_nr_functions = 0
    max_nr_functions_in_interval_max_up_time = 0
    max_nr_functions_in_interval_max_down_time = 0
    max_up_time_found = False
    max_up_time_found_was_true = False
    max_down_time_found = False
    for interval, functions in intervals_download_init_file.items():
        for function in functions:
            if function['function_id'] == function_max_download_time:
                if max_nr_functions_in_interval_max_down_time < len(functions):
                    biggest_interval_max_down_time = interval
                    max_nr_functions_in_interval_max_down_time = len(functions)

    for interval, functions in intervals_upload_sorted_file_stage_1.items():
        if functions[len(functions) - 1]['function_id'] == function_max_upload_time:
            entry_interval_max_up_time = interval
            entry_interval_max_up_time_nr_functions = len(functions)
        for function in functions:
            if function['function_id'] == function_max_upload_time:
                if max_nr_functions_in_interval_max_up_time < len(functions):
                    biggest_interval_max_up_time = interval
                    max_nr_functions_in_interval_max_up_time = len(functions)

    print(f"ENTRY INTERVAL FOR MAX UPLOAD TIME: {entry_interval_max_up_time} -- nr functions: {entry_interval_max_up_time_nr_functions}")
    print(f"BIGGEST INTERVAL MAX UPLOAD TIME: {biggest_interval_max_up_time} -- nr functions: {max_nr_functions_in_interval_max_up_time}")
    # TODO: also find out how many functions were running at the time the max up/down one got into play


    # Create barchart for phase 1
    # print("Creating phase 1 barchart")
    create_barchart(
        ids_with_init_duration_stage1,
        [billed_time_stage_1, init_time_stage_1],
        'Function number vs Billed duration',
        'Function Number',
        'Billed Duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_duration.png',
        enable_legend=True
    )

    # Create barchart for phase 2
    # print("Creating phase 2 barchart")
    create_barchart(
        ids_with_init_duration_stage2,
        [billed_time_stage_2, init_time_stage_2],
        'Function number vs Billed duration',
        'Function Number',
        'Billed Duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_duration.png',
        enable_legend=True
    )

    # print("Creating download_init_file_times_stage_1 barchart")
    create_barchart(
        ids_stage1,
        [download_init_file_times_stage_1],
        'Function number vs Download initial file duration',
        'Function Number',
        'Download initial file duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_file_download_duration.png'
    )

    # print("Creating upload_sorted_file_stage_1 barchart")
    create_barchart(
        ids_stage1,
        [upload_sorted_file_stage_1],
        'Function number vs Upload sorted file phase 1 duration',
        'Function Number',
        'Upload sorted file phase 1 duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_upload_sorted_file_duration.png'
    )

    # print("Create determine_intervals_duration_stage_1 barchart")
    create_barchart(
        ids_stage1,
        [determine_intervals_duration_stage_1],
        'Function number vs determine intervals duration',
        'Function Number',
        'Determine intervals duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_determine_intervals_duration.png'
    )

    print("Create first_sorting_duration_stage_1 barchart")
    create_barchart(
        ids_stage1,
        [first_sorting_duration_stage_1],
        'Function number vs sorting duration',
        'Function Number',
        'Sorting duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_sorting_duration.png'
    )

    # print("Creating download_interval_file_0_stage_2 barchart")
    create_barchart(
        ids_stage2,
        [download_interval_file_0_stage_2],
        'Function number vs Download partition from same file by each function duration',
        'Function Number',
        'Download partition from same file by each function duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_partition_0_duration.png'
    )

    # print("Creating download_interval_file_180_stage_2 barchart")
    # create_barchart(
    #     ids_stage2,
    #     [download_interval_file_180_stage_2],
    #     'Function number vs Download partition 180 duration',
    #     'Function Number',
    #     'Download partition 180 duration (sec)',
    #     'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_download_partition_180_duration.png'
    # )
    #
    # print("Creating download_last_interval_file_stage_2 barchart")
    # create_barchart(
    #     ids_stage2,
    #     [download_last_interval_file_stage_2],
    #     'Function number vs Download last partition duration',
    #     'Function Number',
    #     'Download last partition duration (sec)',
    #     'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_download_last_partition_duration.png'
    # )

    # print("Creating download_intervals_function_180_stage_2 barchart")
    create_barchart(
        [i for i in range(0, 10)],
        [download_intervals_function_180_stage_2],
        'Function number vs Download partition 180 duration',
        'Function Number',
        'Sort category duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_intervals_function_180_stage_2.png'
    )

    # print("Creating sort_final_file_stage_2 barchart")
    create_barchart(
        ids_stage2,
        [sort_final_file_stage_2],
        'Function number vs Sort category duration',
        'Function Number',
        'Sort category duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_sort_category_duration.png'
    )

    # print("Creating upload_final_file_stage_2 barchart")
    create_barchart(
        ids_stage2,
        [upload_final_file_stage_2],
        'Function number vs Upload category duration',
        'Function Number',
        'Upload category duration (sec)',
        f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_upload_category_duration.png'
    )

    # print("Saving durations to file")
    with open(
            f'experiments_raw_data/results_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_durations_phases.json',
            'w') as file:
        json.dump(
            {
                'ids_stage1': ids_stage1,
                'ids_stage2': ids_stage2,
                'init_time_stage_1': init_time_stage_1,
                'init_time_stage_2': init_time_stage_2,
                'billed_time_stage_1': billed_time_stage_1,
                'billed_time_stage_2': billed_time_stage_2,
                'execution_time_stage_1': execution_time_stage_1,
                'execution_time_stage_2': execution_time_stage_2,
                'download_init_file_times_stage_1': download_init_file_times_stage_1,
                'upload_sorted_file_stage_1': upload_sorted_file_stage_1,
                'download_interval_file_0_stage_2': download_interval_file_0_stage_2,
                # 'download_interval_file_180_stage_2': download_interval_file_180_stage_2,
                # 'download_last_interval_file_stage_2': download_last_interval_file_stage_2,
                'download_intervals_function_180_stage_2': download_intervals_function_180_stage_2,
                'sort_final_file_stage_2': sort_final_file_stage_2,
                'upload_final_file_stage_2': upload_final_file_stage_2,
                'experiment_config': EXPERIMENT_CONFIG,
                'experiment_number': experiment_number
            }, file)

    # print("DONE creating plots")
    return "SUCCESSFUL"
