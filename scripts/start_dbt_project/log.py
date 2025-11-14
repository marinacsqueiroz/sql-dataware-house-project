# -----------------------------------------------------------------------------
# This class, LogManager, is designed to set up and manage the standard Python
# logging system, ensuring reliable and organized log file creation.
#
# Step-by-step:
# 1. Initialization (__init__): Accepts a 'subdirectory' name (defaulting to
#    'main') to organize logs. It immediately determines the log path and
#    configures the logger.
# 2. Path Determination (_create_log_directory): Dynamically finds the project's
#    root folder. It handles both standard execution and environments where the
#    script is compiled into a frozen executable (e.g., PyInstaller). It creates
#    the necessary directory structure: './logs/<subdirectory>'.
# 3. File Naming (_create_log_paths): Generates the full file path, naming the
#    log file after the current date (YYYY-MM-DD.log).
# 4. Logger Configuration (_configure_logger): Initializes a named logger, sets
#    the level to DEBUG to capture all messages, and attaches a FileHandler to
#    write logs to the daily file using UTF-8 encoding. It includes a check to
#    prevent the addition of duplicate handlers if the LogManager is initialized
#    multiple times.
# 5. Usage (get_logger): Provides the fully configured logger instance to the
#    calling application.
# 6. Cleanup (flush_and_close): Offers an explicit method to flush the log buffer
#    and close all file handlers, ensuring all pending log messages are written
#    to disk before application exit.
#
# Result:
# - A centralized and reliable logging system writing daily, timestamped log
#   files, ready for production use across different execution environments.
# -----------------------------------------------------------------------------

import os
import sys
import logging
from datetime import datetime

class LogManager:
    def __init__(self, subdirectory='main'):
        self.subdirectory = subdirectory
        self.logger = None
        self.log_path = self._create_log_paths()
        self._configure_logger()

    def _create_log_directory(self):
        if getattr(sys, 'frozen', False):
            project_root = os.path.dirname(sys.executable)
        else:
            project_root = os.path.dirname(os.path.abspath(__file__))
        
        base_dir = os.path.join(project_root, 'logs')
        full_path = os.path.join(base_dir, self.subdirectory)

        os.makedirs(full_path, exist_ok=True)
        return full_path

    def _create_log_paths(self):
        log_directory = self._create_log_directory()
        today_date = datetime.now().strftime('%Y-%m-%d')
        log_path = os.path.join(log_directory, f"{today_date}.log")
        return log_path

    def _configure_logger(self):
        self.logger = logging.getLogger(f'logger_{self.subdirectory}')
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler = logging.FileHandler(self.log_path, encoding='utf-8')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger

    def flush_and_close(self):
        for handler in self.logger.handlers:
            handler.flush()
            handler.close()