import collections
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


def gen_ecdf_host_submit(stats, savepath):
    host_job_create_tstamp = min([cm['host_job_create_tstamp'] for cm in stats])
    submit_times = []
    for function_data in stats:
        submit_time = datetime.fromtimestamp(function_data['host_submit_tstamp']) - datetime.fromtimestamp(host_job_create_tstamp)
        dif_sec = submit_time.total_seconds()
        submit_times.append(dif_sec)

    generate_ecdf(submit_times, savepath)


def generate_ecdf(data, savepath):
    x = np.sort(data)
    n = x.size
    y = np.arange(1, n+1)/n
    plt.scatter(x=x, y=y)
    plt.xlabel('x', fontsize=16)
    plt.ylabel('y', fontsize=16)
    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')
    plt.clf()


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

    sorted_stage1_list = sorted(stage_1_list, key=lambda k: k['function_id'])
    sorted_stage2_list = sorted(stage_2_list, key=lambda k: k['function_id'])

    # gen_ecdf_host_submit(
    #     sorted_stage1_list, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_host_submit_stage1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # return
    # print("Creating timeline stage 1")
    # create_timeline_stage_1(sorted_stage1_list,
    #                         f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_host_submit_time_stage1_{EXPERIMENT_CONFIG}_{experiment_number}')
    # plt.clf()
    # # exit()
    #
    # # print("Creating timeline stage 2")
    # create_timeline_stage_2(sorted_stage2_list,
    #                         f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/stage2_{EXPERIMENT_CONFIG}_{experiment_number}')
    # plt.clf()

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
        init_time_stage_1.append(sorted_stage1_list[i]['init_duration_float'])

    init_time_stage_2 = []
    for i in range(0, len(sorted_stage2_list)):
        init_time_stage_2.append(sorted_stage2_list[i]['init_duration_float'])

    # for i in range(0, 1000):
    #     sorted_stage2_list = get_time_difference(
    #         sorted_stage2_list,
    #         f'start_download_interval_file_{i}_timestamp',
    #         f'finish_download_interval_file_{i}_timestamp',
    #         f'download_interval_file_{i}_duration'
    #     )
    sorted_stage1_list = get_time_difference(
        sorted_stage1_list,
        'worker_end_tstamp',
        'host_result_done_tstamp',
        'fetch_results_time'
    )

    fetch_results_stage_1 = [item['fetch_results_time'] for item in sorted_stage1_list]

    generate_ecdf(fetch_results_stage_1, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_fetch_results_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')

    return
    intervals_durations = {}
    for i in range(0, 1000):
        intervals_durations[i] = [item[f'download_interval_file_{i}_duration'] for item in sorted_stage2_list]

    with open(
            f'experiments_raw_data/intervals_duration_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_durations_phases.json',
            'w') as file:
        json.dump(intervals_durations, file)

    return
    # generate_ecdf(init_time_stage_1, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_init_time_stage1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # generate_ecdf(init_time_stage_2, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_init_time_stage2_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # return

    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'host_submit_tstamp',
    #     'worker_start_tstamp',
    #     'initialization_delay'
    # )
    #
    # sorted_stage2_list = get_time_difference(
    #     sorted_stage2_list,
    #     'host_submit_tstamp',
    #     'worker_start_tstamp',
    #     'initialization_delay'
    # )
    #
    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'host_submit_tstamp',
    #     'host_result_done_tstamp',
    #     'result_done_time'
    # )
    #
    # result_done_time_stage_1 = [item['result_done_time'] for item in sorted_stage1_list]
    #
    # generate_ecdf(result_done_time_stage_1,  f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/result_done_time_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # return

    # initialization_delay_stage_1 = [item['initialization_delay'] for item in sorted_stage1_list]
    # initialization_delay_stage_2 = [item['initialization_delay'] for item in sorted_stage2_list]
    #
    # generate_ecdf(initialization_delay_stage_1,
    #               f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/initialization_delay_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # generate_ecdf(initialization_delay_stage_2,
    #               f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/initialization_delay_stage_2_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    # return

    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'start_downloading_initial_file_timestamp',
    #     'finish_downloading_initial_file_timestamp',
    #     'download_initial_file_duration'
    # )
    #
    # download_initial_file_times = [item['download_initial_file_duration'] for item in sorted_stage1_list]
    # generate_ecdf(download_initial_file_times, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_download_init_file_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    #
    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'start_uploading_first_sorted_file_timestamp',
    #     'finish_uploading_first_sorted_file_timestamp',
    #     'upload_initial_file_duration',
    # )
    #
    # upload_first_sorted_file_times = [item['upload_initial_file_duration'] for item in sorted_stage1_list]
    # generate_ecdf(upload_first_sorted_file_times, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_upload_first_sorted_file_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    #
    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'finish_uploading_first_sorted_file_timestamp',
    #     'finish_determine_categories_phase_timestamp',
    #     'determine_intervals_duration'
    # )
    #
    # determine_intervals_duration_stage_1 = [item['determine_intervals_duration'] for item in sorted_stage1_list]
    # generate_ecdf(determine_intervals_duration_stage_1, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_determine_intervals_stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')
    #
    # sorted_stage1_list = get_time_difference(
    #     sorted_stage1_list,
    #     'start_first_sorting_timestamp',
    #     'finish_first_sorting_timestamp',
    #     'first_sorting_duration'
    # )
    #
    # first_sorting_duration_stage_1 = [item['first_sorting_duration'] for item in sorted_stage1_list]
    # generate_ecdf(first_sorting_duration_stage_1, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_first_sorting__stage_1_{EXPERIMENT_CONFIG}_{experiment_number}.png')

    # return
    #
    # # Donwload interval from same file by all functions
    # sorted_stage2_list = get_time_difference(
    #     sorted_stage2_list,
    #     'start_download_interval_file_0_timestamp',
    #     'finish_download_interval_file_0_timestamp',
    #     'download_interval_file_0_duration'
    # )
    # download_interval_file_0_stage_2 = [item['download_interval_file_0_duration'] for item in sorted_stage2_list]
    #
    # sorted_stage2_list = get_time_difference(
    #     sorted_stage2_list,
    #     'start_download_interval_file_256_timestamp',
    #     'finish_download_interval_file_256_timestamp',
    #     'download_interval_file_256_duration'
    # )
    # download_interval_file_256_stage2 = [item['download_interval_file_256_duration'] for item in sorted_stage2_list]
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
    #
    # sorted_stage2_list = get_time_difference(
    #     sorted_stage2_list,
    #     'start_download_intervals_timestamp',
    #     'finish_download_intervals_timestamp',
    #     'download_intervals_duration'
    # )
    #
    # download_intervals_durations = [item['download_intervals_duration'] for item in sorted_stage2_list]
    #
    # # Download intervals from every file same function(0, 256)
    # # download_intervals_function_256_stage_2 = []
    # download_intervals_function_0_stage_2 = []
    # # function_256 = sorted_stage2_list[256]
    # function_0 = sorted_stage2_list[0]
    # for i in range(0, 100):
    #     start_key = f'start_download_interval_file_{i}_timestamp'
    #     finish_key = f'finish_download_interval_file_{i}_timestamp'
    #     # dif_256 = datetime.fromtimestamp(function_256[finish_key]) - datetime.fromtimestamp(function_256[start_key])
    #     # dif_256_sec = dif_256.total_seconds()
    #     # download_intervals_function_256_stage_2.append(dif_256_sec)
    #
    #     dif_0 = datetime.fromtimestamp(function_0[finish_key]) - datetime.fromtimestamp(function_0[start_key])
    #     dif_0_sec = dif_0.total_seconds()
    #     download_intervals_function_0_stage_2.append(dif_0_sec)
    #
    sorted_stage2_list = get_time_difference(
        sorted_stage2_list,
        'start_sort_final_file_timestamp',
        'finish_sort_final_file_timestamp',
        'sort_final_file_duration'
    )
    sort_final_file_stage_2 = [item['sort_final_file_duration'] for item in sorted_stage2_list]
    generate_ecdf(sort_final_file_stage_2, f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/ecdf_final_sorting_stage_2_{EXPERIMENT_CONFIG}_{experiment_number}.png')

    #
    # sorted_stage2_list = get_time_difference(
    #     sorted_stage2_list,
    #     'start_write_final_file_timestamp',
    #     'finish_write_final_file_timestamp',
    #     'write_final_file_duration'
    # )
    # upload_final_file_stage_2 = [item['write_final_file_duration'] for item in sorted_stage2_list]
    #
    #
    # upload_sorted_file_stage_1 = [item['upload_initial_file_duration'] for item in sorted_stage1_list]
    # download_init_file_times_stage_1 = [item['download_initial_file_duration'] for item in sorted_stage1_list]
    # determine_intervals_stage_1 = [item['determine_intervals_duration'] for item in sorted_stage1_list]
    # # Get max execution time, download init and upload init time
    # function_max_exec_time = 0
    # function_id_max_download_time = 0
    # function_id_max_upload_time = 0
    # max_exec_time = 0.0
    # max_down_time = 0.0
    # max_up_time = 0.0
    # down_time_for_max_exec_time = 0.0
    # up_time_for_max_exec_time = 0.0
    # exec_time_for_max_down_time = 0.0
    # up_time_for_max_down_time = 0.0
    # exec_time_for_max_up_time = 0.0
    # down_time_for_max_up_time = 0.0
    # interval_for_max_exec_time = 0.0
    # interval_for_max_up_time = 0.0
    # interval_for_max_down_time = 0.0
    # sorting_for_max_exec_time = 0.0
    # sorting_for_max_up_time = 0.0
    # sorting_for_max_down_time = 0.0
    #
    # for i in range(0, len(execution_time_stage_1)):
    #     if max_up_time < upload_sorted_file_stage_1[i]:
    #         max_up_time = upload_sorted_file_stage_1[i]
    #         function_id_max_upload_time = i
    #         exec_time_for_max_up_time = execution_time_stage_1[i]
    #         down_time_for_max_up_time = download_init_file_times_stage_1[i]
    #         interval_for_max_up_time = determine_intervals_stage_1[i]
    #         sorting_for_max_up_time = first_sorting_duration_stage_1[i]
    #
    #     if max_down_time < download_init_file_times_stage_1[i]:
    #         max_down_time = download_init_file_times_stage_1[i]
    #         function_id_max_download_time = i
    #         exec_time_for_max_down_time = execution_time_stage_1[i]
    #         up_time_for_max_down_time = upload_sorted_file_stage_1[i]
    #         interval_for_max_down_time = determine_intervals_stage_1[i]
    #         sorting_for_max_down_time = first_sorting_duration_stage_1[i]
    #
    #     if max_exec_time < execution_time_stage_1[i]:
    #         max_exec_time = execution_time_stage_1[i]
    #         function_max_exec_time = i
    #         down_time_for_max_exec_time = download_init_file_times_stage_1[i]
    #         up_time_for_max_exec_time = upload_sorted_file_stage_1[i]
    #         interval_for_max_exec_time = determine_intervals_stage_1[i]
    #         sorting_for_max_exec_time = first_sorting_duration_stage_1[i]
    #
    # print(
    #     f"MAX EXEC TIME {max_exec_time} on function {function_max_exec_time} with download time {down_time_for_max_exec_time} and upload time {up_time_for_max_exec_time} and interval time {interval_for_max_exec_time} and sorting time {sorting_for_max_exec_time}")
    # print(
    #     f'MAX DOWN TIME {max_down_time} on function {function_id_max_download_time} with upload time {up_time_for_max_down_time} and exec time {exec_time_for_max_down_time} and interval time {interval_for_max_down_time} and sorting time {sorting_for_max_down_time}')
    # print(
    #     f'MAX UP TIME {max_up_time} on function {function_id_max_upload_time} with download time {down_time_for_max_up_time} and exec time {exec_time_for_max_up_time} and interval time {interval_for_max_up_time} and sorting time {sorting_for_max_up_time}')
    # exit()
    #
    # # Overlap timestamps
    # intervals_download_init_file = {}
    # intervals_upload_sorted_file_stage_1 = {}
    # for function_record in sorted_stage1_list:
    #     intervals_download_init_file.update(
    #         {
    #             function_record['start_downloading_initial_file_timestamp']: {
    #                 'function': {
    #                     "start_downloading_initial_file_timestamp": function_record[
    #                         'start_downloading_initial_file_timestamp'],
    #                     "finish_downloading_initial_file_timestamp": function_record[
    #                         'finish_downloading_initial_file_timestamp'],
    #                     "function_id": function_record['function_id']
    #                 },
    #                 'array_functions': []
    #             }
    #         }
    #     )
    #     intervals_upload_sorted_file_stage_1.update(
    #         {
    #             function_record['start_uploading_first_sorted_file_timestamp']: {
    #                 'function': {
    #                     "start_uploading_first_sorted_file_timestamp": function_record[
    #                         'start_uploading_first_sorted_file_timestamp'],
    #                     "finish_uploading_first_sorted_file_timestamp": function_record[
    #                         'finish_uploading_first_sorted_file_timestamp'],
    #                     "function_id": function_record['function_id']
    #                 },
    #                 'array_functions': []
    #             }
    #         }
    #     )
    #
    #     for start_time, functions in intervals_download_init_file.items():
    #         if len(functions) > 0:
    #             if start_time <= function_record['start_downloading_initial_file_timestamp'] < functions['function']['finish_downloading_initial_file_timestamp']:
    #                 functions['array_functions'].append(
    #                     {
    #                         "start_downloading_initial_file_timestamp": function_record[
    #                             'start_downloading_initial_file_timestamp'],
    #                         "finish_downloading_initial_file_timestamp": function_record[
    #                             'finish_downloading_initial_file_timestamp'],
    #                         "function_id": function_record['function_id']
    #                     }
    #                 )
    #         else:
    #             functions['array_functions'].append(
    #                 {
    #                     "start_downloading_initial_file_timestamp": function_record[
    #                         'start_downloading_initial_file_timestamp'],
    #                     "finish_downloading_initial_file_timestamp": function_record[
    #                         'finish_downloading_initial_file_timestamp'],
    #                     "function_id": function_record['function_id']
    #                 }
    #             )
    #
    #     for start_time, functions in intervals_upload_sorted_file_stage_1.items():
    #         if len(functions) > 0:
    #             if start_time <= function_record['start_uploading_first_sorted_file_timestamp'] < functions['function']['finish_uploading_first_sorted_file_timestamp']:
    #                 functions['array_functions'].append(
    #                     {
    #                         "start_uploading_first_sorted_file_timestamp": function_record[
    #                             'start_uploading_first_sorted_file_timestamp'],
    #                         "finish_uploading_first_sorted_file_timestamp": function_record[
    #                             'finish_uploading_first_sorted_file_timestamp'],
    #                         "function_id": function_record['function_id']
    #                     }
    #                 )
    #         else:
    #             functions['array_functions'].append(
    #                 {
    #                     "start_uploading_first_sorted_file_timestamp": function_record[
    #                         'start_uploading_first_sorted_file_timestamp'],
    #                     "finish_uploading_first_sorted_file_timestamp": function_record[
    #                         'finish_uploading_first_sorted_file_timestamp'],
    #                     "function_id": function_record['function_id']
    #                 }
    #             )
    #
    # # Determine the biggest interval from which the function with biggest upload/download time is part of
    # biggest_interval_max_up_time = None
    # biggest_interval_max_down_time = None
    # max_nr_functions_in_interval_max_up_time = 0
    # max_nr_functions_in_interval_max_down_time = 0
    #
    # # Order intervals by start_time
    #
    # intervals_download_init_file = collections.OrderedDict(sorted(intervals_download_init_file.items()))
    # intervals_upload_sorted_file_stage_1 = collections.OrderedDict(sorted(intervals_upload_sorted_file_stage_1.items()))
    #
    # for start_time, functions in intervals_download_init_file.items():
    #     for i, function in enumerate(functions['array_functions']):
    #         if function['function_id'] == function_id_max_download_time:
    #             if max_nr_functions_in_interval_max_down_time < len(functions['array_functions']):
    #                 biggest_interval_max_down_time = functions['function']
    #                 max_nr_functions_in_interval_max_down_time = len(functions['array_functions'])
    #
    # for start_time, functions in intervals_upload_sorted_file_stage_1.items():
    #     for i, function in enumerate(functions['array_functions']):
    #         if function['function_id'] == function_id_max_upload_time:
    #             if max_nr_functions_in_interval_max_up_time < len(functions['array_functions']):
    #                 biggest_interval_max_up_time = functions['function']
    #                 max_nr_functions_in_interval_max_up_time = len(functions['array_functions'])
    #
    # max_up_time_function = sorted_stage1_list[function_id_max_upload_time]
    # max_down_time_function = sorted_stage1_list[function_id_max_download_time]
    # nr_functions_running_when_max_up_time = 0
    # nr_functions_running_when_max_down_time = 0
    # for fct in sorted_stage1_list:
    #     if fct['start_downloading_initial_file_timestamp'] < max_up_time_function['start_downloading_initial_file_timestamp'] < fct['finish_downloading_initial_file_timestamp']:
    #         nr_functions_running_when_max_up_time += 1
    #     if fct['start_uploading_first_sorted_file_timestamp'] < max_down_time_function['start_uploading_first_sorted_file_timestamp'] < fct['finish_uploading_first_sorted_file_timestamp']:
    #         nr_functions_running_when_max_down_time += 1
    # print('=======================================')
    # print(f"MAX UP TIME already running functions: {nr_functions_running_when_max_up_time}")
    # print(f"BIGGEST INTERVAL MAX UPLOAD TIME: interval of function number {biggest_interval_max_up_time['function_id']} -- nr functions: {max_nr_functions_in_interval_max_up_time}")
    # print('---------------------------------------')
    # print(f"MAX DOWN TIME -- already running functions: {nr_functions_running_when_max_down_time}")
    # print(f"BIGGEST INTERVAL FOR MAX DOWN TIME: interval of function number {biggest_interval_max_down_time['function_id']} -- nr functions: {max_nr_functions_in_interval_max_down_time}")
    # print('=======================================')
    #
    # functions_started = []
    # for fct1 in sorted_stage1_list:
    #     temp_record = {
    #         'function': fct1,
    #         'nr_started_functions_upload': 0,
    #         'nr_started_functions_download': 0
    #     }
    #     for fct2 in sorted_stage1_list:
    #         if fct2['start_uploading_first_sorted_file_timestamp'] < fct1['start_uploading_first_sorted_file_timestamp'] < fct2['finish_uploading_first_sorted_file_timestamp']:
    #             temp_record['nr_started_functions_upload'] += 1
    #         if fct2['start_downloading_initial_file_timestamp'] < fct1['start_downloading_initial_file_timestamp'] < fct2['finish_downloading_initial_file_timestamp']:
    #             temp_record['nr_started_functions_download'] += 1
    #     functions_started.append(temp_record)
    #
    # stats_max_concurrent_upload = {'function': None, 'max_concurrent_uploads': 0}
    # stats_max_concurrent_download = {'function': None, 'max_concurrent_downloads': 0}
    #
    # for function_record in functions_started:
    #     if stats_max_concurrent_upload['max_concurrent_uploads'] < function_record['nr_started_functions_upload']:
    #         stats_max_concurrent_upload['max_concurrent_uploads'] = function_record['nr_started_functions_upload']
    #         stats_max_concurrent_upload['function'] = function_record['function']
    #
    #     if stats_max_concurrent_download['max_concurrent_downloads'] < function_record['nr_started_functions_download']:
    #         stats_max_concurrent_download['max_concurrent_downloads'] = function_record['nr_started_functions_download']
    #         stats_max_concurrent_download['function'] = function_record['function']
    #
    # print(f"MAX CONCURRENT UPLOADS: {stats_max_concurrent_upload['max_concurrent_uploads']} -- function: {stats_max_concurrent_upload['function']['function_id']} -- upload function time: {stats_max_concurrent_upload['function']['upload_initial_file_duration']}")
    # print(f"MAX CONCURRENT DOWNLOADS: {stats_max_concurrent_download['max_concurrent_downloads']} -- function: {stats_max_concurrent_download['function']['function_id']} -- download function time: {stats_max_concurrent_upload['function']['download_initial_file_duration']}")
    # print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    # return
    #
    # # Create barchart for phase 1
    # print("Creating phase 1 barchart")
    # create_barchart(
    #     ids_with_init_duration_stage1,
    #     [billed_time_stage_1, init_time_stage_1],
    #     'Function number vs Billed duration',
    #     'Function Number',
    #     'Billed Duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_duration.png',
    #     enable_legend=True
    # )
    #
    # # Create barchart for phase 2
    # # print("Creating phase 2 barchart")
    # create_barchart(
    #     ids_with_init_duration_stage2,
    #     [billed_time_stage_2, init_time_stage_2],
    #     'Function number vs Billed duration',
    #     'Function Number',
    #     'Billed Duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_duration.png',
    #     enable_legend=True
    # )
    #
    # # print("Creating download_init_file_times_stage_1 barchart")
    # create_barchart(
    #     ids_stage1,
    #     [download_init_file_times_stage_1],
    #     'Function number vs Download initial file duration',
    #     'Function Number',
    #     'Download initial file duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_init_file_download_duration.png'
    # )
    #
    # # print("Creating upload_sorted_file_stage_1 barchart")
    # create_barchart(
    #     ids_stage1,
    #     [upload_sorted_file_stage_1],
    #     'Function number vs Upload sorted file phase 1 duration',
    #     'Function Number',
    #     'Upload sorted file phase 1 duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_upload_sorted_file_duration.png'
    # )
    #
    # # print("Create determine_intervals_duration_stage_1 barchart")
    # create_barchart(
    #     ids_stage1,
    #     [determine_intervals_duration_stage_1],
    #     'Function number vs determine intervals duration',
    #     'Function Number',
    #     'Determine intervals duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_determine_intervals_duration.png'
    # )
    #
    # print("Create first_sorting_duration_stage_1 barchart")
    # create_barchart(
    #     ids_stage1,
    #     [first_sorting_duration_stage_1],
    #     'Function number vs sorting duration',
    #     'Function Number',
    #     'Sorting duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_1_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_sorting_duration.png'
    # )
    #
    # # print("Creating download_interval_file_0_stage_2 barchart")
    # create_barchart(
    #     ids_stage2,
    #     [download_interval_file_0_stage_2],
    #     'Function number vs Duration of download partition from file 0 by each function',
    #     'Function Number',
    #     'Download partition from file 0 by each function duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_file_0_duration.png'
    # )
    #
    # # create_barchart(
    # #     ids_stage2,
    # #     [download_interval_file_256_stage2],
    # #     'Function number vs Duration of download partition from file 256 by each function',
    # #     'Function Number',
    # #     'Download partitions from file 256 by each function duration (sec)',
    # #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_file_256_duration.png'
    # # )
    #
    # print("Creating download_intervals_function_0_stage_2 barchart")
    # create_barchart(
    #     [i for i in range(0, 100)],
    #     [download_intervals_function_0_stage_2],
    #     'Function number vs Duration of download partitions by function 0',
    #     'Function Number',
    #     'Duration of download partitions by function 0 (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_intervals_function_0_stage_2.png'
    # )
    #
    # # print("Creating download_intervals_function_256_stage_2 barchart")
    # # create_barchart(
    # #     [i for i in range(0, 100)],
    # #     [download_intervals_function_256_stage_2],
    # #     'Function number vs Duration of download partitions by function 256',
    # #     'Function Number',
    # #     'Duration of download partitions by function 256 (sec)',
    # #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_intervals_function_256_stage_2.png'
    # # )
    #
    # create_barchart(
    #     ids_stage2,
    #     [download_intervals_durations],
    #     'Function number vs Download all intervals duration',
    #     'Function Number',
    #     'Download intervals duration',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_download_intervals_duration.png'
    # )
    #
    # # print("Creating sort_final_file_stage_2 barchart")
    # create_barchart(
    #     ids_stage2,
    #     [sort_final_file_stage_2],
    #     'Function number vs Sort category duration',
    #     'Function Number',
    #     'Sort category duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_sort_category_duration.png'
    # )
    #
    # # print("Creating upload_final_file_stage_2 barchart")
    # create_barchart(
    #     ids_stage2,
    #     [upload_final_file_stage_2],
    #     'Function number vs Upload category duration',
    #     'Function Number',
    #     'Upload category duration (sec)',
    #     f'plots_{EXPERIMENT_CONFIG}_{nr_intervals}/barchart_phase_2_{EXPERIMENT_CONFIG}_intervals_{nr_intervals}_expnr_{experiment_number}_upload_category_duration.png'
    # )

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
                # 'download_init_file_times_stage_1': download_initial_file_times,
                # 'upload_sorted_file_stage_1': upload_first_sorted_file_times,
                # 'determine_intervals_duration_stage_1': determine_intervals_duration_stage_1,
                # 'first_sorting_duration_stage_1': first_sorting_duration_stage_1,
                # 'download_interval_file_0_stage_2': download_interval_file_0_stage_2,
                # 'download_interval_file_180_stage_2': download_interval_file_180_stage_2,
                # 'download_last_interval_file_stage_2': download_last_interval_file_stage_2,
                # 'download_intervals_function_256_stage_2': download_intervals_function_256_stage_2,
                'sort_final_file_stage_2': sort_final_file_stage_2,
                # 'upload_final_file_stage_2': upload_final_file_stage_2,
                'experiment_config': EXPERIMENT_CONFIG,
                'experiment_number': experiment_number
            }, file)


    # print("DONE creating plots")
    return "SUCCESSFUL"
