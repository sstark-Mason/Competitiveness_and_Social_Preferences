import sys
import datetime
import logging
from colorama import Fore, Back, Style, init

def configure_logger(log_name, console_level='INFO', file_level='WARNING', sorting=False):

    # if not os.path.exists('_Logs/'):
    #     os.makedirs('_Logs/')

    class SortedBySeverityHandler(logging.StreamHandler):
        def __init__(self, stream=None):
            super().__init__(stream)
            self.log_records = []

        def emit(self, record):
            self.log_records.append(record)

        def flush(self):
            if self.log_records:
                sorted_records = sorted(self.log_records, key=lambda r: r.levelno)
                for record in sorted_records:
                    sys.stdout.write(self.format(record) + '\n')
                self.log_records = []
            super().flush()

    init(autoreset=True)

    class ColoredFormatter(logging.Formatter):
        COLORS = {
            'DEBUG': Fore.CYAN,
            'INFO': Fore.BLUE,
            'WARNING': Fore.YELLOW,
            'ERROR': Fore.RED + Back.WHITE,
            'CRITICAL': Fore.BLACK + Style.BRIGHT + Back.WHITE,
            'TRACE': Fore.MAGENTA + Style.BRIGHT + Back.BLACK,
        }

        def format(self, record):
            log_color = self.COLORS.get(record.levelname, "")
            log_message = super().format(record)
            return f"{log_color}{log_message}{Style.RESET_ALL}"

    TRACE = 55
    logging.addLevelName(TRACE, 'TRACE')
    class TraceLogger(logging.Logger):
        def trace(self, msg, *args, **kwargs):
            if self.isEnabledFor(TRACE):
                self._log(TRACE, msg, args, **kwargs)

    logging.Logger.trace = TraceLogger.trace

    console_formatter = ColoredFormatter('== %(name)s / %(funcName)s() / %(levelname)s ==    %(message)s')
    logger = logging.getLogger(log_name)  # Sets up logger. __name__ usually recommended; not using here because this is a module.
    logger.setLevel(logging.DEBUG)  # Sets effective logging level (unsure if this is redundant when setting file and console handlers).

    # console_handler = logging.StreamHandler()  # Defaults to sys.stderr; all prints at top of console, all logs at bottom

    if sorting:
        console_handler = SortedBySeverityHandler(sys.stdout)
    else:
        console_handler = logging.StreamHandler(sys.stdout)  # sys.stdout - Combines logging and printing to same console
    console_handler.setLevel(console_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # file_handler = logging.FileHandler(f'{datetime.datetime.now().strftime("%Y.%m.%d - %H.%M.%S")} - {log_name}.log', encoding='UTF8')
    file_handler = logging.FileHandler(f'{log_name}.log', encoding='UTF8')
    file_handler.setLevel(file_level)
    logger.addHandler(file_handler)

    return logger


def log_test():
    # logger = configure_logger('Logger_Test')
    #
    # logger.debug('DEBUG')
    # logger.info('INFO')
    # logger.warning('WARNING')
    # logger.error('ERROR')
    # logger.critical('CRITICAL')
    # print('Print statement\n')
    # logger.critical('CRITICAL')
    # logger.error('ERROR')
    # logger.warning('WARNING')
    # logger.info('INFO')
    # logger.debug('DEBUG')
    # logger.trace('TRACE')

    log = configure_logger('Logger_Test', sorting=False)

    log.debug('DEBUG')
    log.info('INFO')
    log.warning('WARNING')
    log.error('ERROR')
    log.critical('CRITICAL')
    print('Print statement')
    log.trace('TRACE')
    log.critical('CRITICAL')
    log.error('ERROR')
    log.warning('WARNING')
    log.info('INFO')
    log.debug('DEBUG')

    input()

if __name__ == '__main__':
    log_test()


logger = configure_logger('_Logs', console_level='DEBUG', file_level='WARNING', sorting=False)
