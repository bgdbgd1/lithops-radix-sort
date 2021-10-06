import time

from create_boxplots_multiple_experiments import create_boxplots
from process_logs import process_logs
from create_plots import create_plots


def run_create_plots(first_process_logs=False):
    if first_process_logs:
        process_logs(
            EXECUTE_DOWNLOAD_LOGGING=True,
            EXECUTE_PROCESS_LOGGING=True,
            REMOVE_S3_LOGGING=False,
            REMOVE_LOCAL_LOGGING=False
        )
    res = create_plots(EXPERIMENT_CONFIG='', experiment_number=0)
    if 'Functions without ids' in res:
        print(res)
        time.sleep(120)
        process_logs(
            EXECUTE_DOWNLOAD_LOGGING=True,
            EXECUTE_PROCESS_LOGGING=True,
            REMOVE_S3_LOGGING=False,
            REMOVE_LOCAL_LOGGING=False
        )


if __name__ == '__main__':
    create_boxplots('10mb-10files-binary')
    # for i in range(1, 3):
    #     create_plots('10mb-10files-binary', i)
    # process_logs(
    #     EXECUTE_DOWNLOAD_LOGGING=False,
    #     EXECUTE_PROCESS_LOGGING=True,
    #     REMOVE_S3_LOGGING=False,
    #     REMOVE_LOCAL_LOGGING=False
    # )
