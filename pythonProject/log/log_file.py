import datetime
import logging
import os
import shutil
from pathlib import Path


def setup_logger():
    if os.environ.get('TEST_NAME'):
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
        level = logging.INFO
        log_file_path = f"{Path(__file__).cwd().parent.parent.parent}/log/"
        log_file_path = f"{Path(__file__).cwd()}/log/"
        file_name = f"{log_file_path}{os.environ.get('TEST_NAME')}.log"
        handler = logging.FileHandler(file_name)
        handler.setFormatter(formatter)

        logger = logging.getLogger(os.environ.get('TEST_NAME'))
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger


def delete_log_files():
    log_files_path = f"{Path(__file__).cwd()}/log/"
    files_list = [f for f in os.listdir(log_files_path) if f.endswith(".log")]
    for f in files_list:
        os.remove(os.path.join(log_files_path, f))


def archive_logs():
    try:
        create_new_archive_folder()
        move_logs_to_archive()
    except FileExistsError:  # fail when run in parallel
        pass
    except FileNotFoundError:
        pass


def create_new_archive_folder():
    global current_archive_folder
    now = datetime.datetime.now()
    time_stamp = f"{now.year}{now.month}{now.day}{now.hour}{now.minute}{now.second}"
    archive_log_files_path = f"{Path(__file__).cwd().parent.parent}/log/log_archive"
    current_archive_folder = f"{archive_log_files_path}/{'log_package_'}{time_stamp}/"
    os.mkdir(f"{archive_log_files_path}/{'log_package_'}{time_stamp}")


def move_logs_to_archive():
    log_files_path = f"{Path(__file__).cwd().parent.parent}/log/"
    files_list = [f for f in os.listdir(log_files_path) if f.endswith(".log")]
    for log_file in files_list:
        shutil.move(log_files_path + log_file, current_archive_folder)


def delete_archive():
    archive_folders_path = f"{Path(__file__).cwd().parent}/log/log_archive/"
    folder_list = [f for f in os.listdir(archive_folders_path) if "_init_" not in f]
    for folder in folder_list:
        shutil.rmtree(archive_folders_path + folder)