import collections
import matplotlib.pyplot as plt
import json
import numpy as np


def bulk_gen_ecdf(data, savepath):
    plt.xlabel('x', fontsize=16)
    plt.ylabel('y', fontsize=16)

    for exp_data in data:
        x = np.sort(exp_data)
        n = x.size
        y = np.arange(1, n+1) / n
        plt.scatter(x=x, y=y)

    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')
    plt.clf()


def gen_ecdf(data, savepath):
    x = np.sort(data)
    n = x.size
    y = np.arange(1, n+1)/n
    plt.scatter(x=x, y=y)
    plt.xlabel('x', fontsize=16)
    plt.ylabel('y', fontsize=16)
    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')
    plt.clf()


def generate_ecdf_all_intervals_all_functions(experiment_config, nr_intervals):
    all_intervals_data = []
    for i in range(1, 11):
        with open(f'experiments_raw_data/intervals_duration_{experiment_config}_intervals_{nr_intervals}_expnr_{i}_durations_phases.json') as file:
            json_file = json.load(file)
            for interval_nr, download_times in json_file.items():
                all_intervals_data += download_times
                # print(f'Experiment Number: {i}, Interval nr: {interval_nr}, length array: {len(download_times)}')


    set_intervals = set(all_intervals_data)
    print("========")
    print(len(all_intervals_data))
    print(len(list(set_intervals)))

    nr_more_than_05 = 0
    nr_more_than_1 = 0
    for val in all_intervals_data:
        if val >= 0.5:
            nr_more_than_05 += 1
        if val >= 1.0:
            nr_more_than_1 += 1

    print(f'nr_more_than_0.5: {nr_more_than_05}')
    print(f'nr_more_than_1: {nr_more_than_1}')

    # gen_ecdf(all_intervals_data, f'plots_{experiment_config}_{nr_intervals}/ecdf_all_intervals_download_time_experiments_1_to_10.png')
    # gen_ecdf(list(set_intervals), f'plots_{experiment_config}_{nr_intervals}/ecdf_all_intervals_set_download_time_experiments_1_to_10.png')


def generate_ecdf(experiment_config, nr_intervals, var_name):
    file_data = []
    times = {}
    # init_times_stage_2 = {}
    for i in range(1, 11):
        with open(f'experiments_raw_data/results_{experiment_config}_intervals_{nr_intervals}_expnr_{i}_durations_phases.json') as file:
            file_data.append(
                {
                    i: json.load(file)
                }
            )

    total_count_higher_2 = 0
    total_count_higher_3 = 0
    total_count_higher_4 = 0
    total_count_higher_5 = 0
    total_count_higher_6 = 0
    total_count_higher_1p5 = 0

    count_higher_2 = 0
    count_higher_3 = 0
    count_higher_4 = 0
    count_higher_5 = 0
    count_higher_6 = 0
    count_higher_1p5 = 0
    for experiment in file_data:
        for exp_nr, exp_data in experiment.items():
            # for value in exp_data[var_name]:
            #     if value >= 6.0:
            #         count_higher_6 += 1
            #         total_count_higher_6 += 1
            #     if value >= 5:
            #         count_higher_5 += 1
            #         total_count_higher_5 += 1
            #     if value >= 4:
            #         count_higher_4 += 1
            #         total_count_higher_4 += 1
            #     if value >= 3:
            #         count_higher_3 += 1
            #         total_count_higher_3 += 1
            #     if value >= 2:
            #         count_higher_2 += 1
            #         total_count_higher_2 += 1
            #     if value >= 1.5:
            #         count_higher_1p5 += 1
            #         total_count_higher_1p5 += 1
            times.update({exp_nr: exp_data[var_name]})
            # init_times_stage_2.update({exp_nr: exp_data['init_time_stage_2']})
        # print(f"Took more than 1.5 seconds: {count_higher_1p5}")
        # print(f"Took more than 2 seconds: {count_higher_2}")
        # print(f"Took more than 3 seconds: {count_higher_3}")
        # print(f"Took more than 4 seconds: {count_higher_4}")
        # print(f"Took more than 5 seconds: {count_higher_5}")
        # print(f"Took more than 6 seconds: {count_higher_6}")
        # print(f"==========================================")

        # count_higher_1p5 = 0
        # count_higher_2 = 0
        # count_higher_3 = 0
        # count_higher_4 = 0
        # count_higher_5 = 0
        # count_higher_6 = 0

    # print("--------------------TOTAL---------------------------")
    # print(f"Took more than 1.5 seconds: {total_count_higher_1p5}")
    # print(f"Took more than 2 seconds: {total_count_higher_2}")
    # print(f"Took more than 3 seconds: {total_count_higher_3}")
    # print(f"Took more than 4 seconds: {total_count_higher_4}")
    # print(f"Took more than 5 seconds: {total_count_higher_5}")
    # print(f"Took more than 6 seconds: {total_count_higher_6}")

    # gen_ecdf(init_times_stage_1[1], f'plots_{experiment_config}_{nr_intervals}/ecdf_init_time_stage_1_expnr_1.png')

    all_data = []
    for key, val in times.items():
        all_data += val

    gen_ecdf(all_data, f'plots_{experiment_config}_{nr_intervals}/ecdf_{var_name}_experiments_1_to_10.png')
    # bulk_gen_ecdf(
    #     [times[i] for i in range(1, 11)],
    #     f'plots_{experiment_config}_{nr_intervals}/ecdf_{var_name}_experiments_2_to_10.png'
    # )

    # bulk_gen_ecdf([init_times_stage_2[i] for i in range(1, 11)], f'plots_{experiment_config}_{nr_intervals}/ecdf_init_time_stage_2_experiments_1_to_10.png')


    # for i in range(1, 11):
    #     init_times_stage_1[i]['np_sorted'] = np.sort(init_times_stage_1[i]['init_time_stage_1'])
    #     init_times_stage_2[i]['np_sorted'] = np.sort(init_times_stage_2[i]['init_time_stage_2'])


def create_boxplots(experiment_config, nr_intervals):
    data = []
    processed_data = {}
    stages = [
        'upload_sorted_file_stage_1',
    ]
    for i in range(1, 11):
        with open(f'experiments_raw_data/results_{experiment_config}_intervals_{nr_intervals}_expnr_{i}_durations_phases.json') as file:
            data.append(json.load(file))

    for experiment_data in data:
        for stage in stages:
            if not processed_data.get(stage):
                processed_data.update(
                    {
                        stage: {experiment_data['experiment_number']: experiment_data[stage]}
                    }
                )
            else:
                processed_data[stage].update({experiment_data['experiment_number']: experiment_data[stage]})

    for stage, experiments_data in processed_data.items():
        ordered_experiments = collections.OrderedDict(sorted(experiments_data.items()))
        fig, ax = plt.subplots()
        ax.boxplot(ordered_experiments.values())
        ax.set_xticklabels(ordered_experiments.keys())
        # for experiment in values:
        #     plt.boxplot(experiment['array_values'])

        plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/plots_{experiment_config}_{nr_intervals}/boxplot_{experiment_config}_{nr_intervals}_{stage}.png')
        # plt.show()
        plt.clf()
