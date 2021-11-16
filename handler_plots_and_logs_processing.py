import time

from create_boxplots_multiple_experiments import create_boxplots, generate_ecdf, \
    generate_ecdf_all_intervals_all_functions
from process_logs import process_logs
from create_plots import create_plots


# def run_create_plots(first_process_logs=False):
#     if first_process_logs:
#         process_logs(
#             EXECUTE_DOWNLOAD_LOGGING=True,
#             EXECUTE_PROCESS_LOGGING=True,
#             REMOVE_S3_LOGGING=False,
#             REMOVE_LOCAL_LOGGING=False
#         )
#     res = create_plots(EXPERIMENT_CONFIG='', experiment_number=0)
#     if 'Functions without ids' in res:
#         print(res)
#         time.sleep(120)
#         process_logs(
#             EXECUTE_DOWNLOAD_LOGGING=True,
#             EXECUTE_PROCESS_LOGGING=True,
#             REMOVE_S3_LOGGING=False,
#             REMOVE_LOCAL_LOGGING=False
#         )


if __name__ == '__main__':

    # create_boxplots('100mb-1000files', 512)
    # generate_ecdf_all_intervals_all_functions('100mb-1000files', 512)
    for i in range(1, 11):
        create_plots('1gb-100files', 512, i)
    # process_logs(
    #     '100mb-1000files',
    #     512,
    #     EXECUTE_DOWNLOAD_LOGGING=False,
    #     EXECUTE_UNZIP=True,
    #     EXECUTE_PROCESS_LOGGING=True,
    #     REMOVE_CLOUDWATCH_LOGGING=False,
    #     REMOVE_S3_LOGGING=False,
    #     REMOVE_LOCAL_LOGGING=False
    # )
