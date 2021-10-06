import collections
import glob
import matplotlib.pyplot as plt
import json


def create_boxplot(data, savepath):
    plt.figure(figsize=(10, 7))

    # Creating plot
    plt.boxplot(data)
    plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/{savepath}')
    # plt.clf()


def create_boxplots(experiment_config):
    data = []
    processed_data = {}
    stages = [
        'download_init_file_times_stage_1',
        'upload_sorted_file_stage_1',
        'download_interval_file_0_stage_2',
        # 'download_interval_file_180_stage_2',
        # 'download_last_interval_file_stage_2',
        'sort_final_file_stage_2',
        'upload_final_file_stage_2'
    ]

    durations_phases_files = glob.glob("/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/experiments_raw_data/*durations_phases.json")

    for file_path in durations_phases_files:
        with open(file_path) as file:
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

        plt.savefig(f'/Users/bogdan/scoala/thesis/repo-lithops-radix-sort/lithops-radix-sort/plots/boxplot_{experiment_config}_{stage}.png')
        # plt.show()
        plt.clf()
