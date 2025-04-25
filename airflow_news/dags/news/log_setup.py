import logging
import logging.config
def setup_logging(log_file_path='/opt/airflow/logs/log.log'):
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'level': 'INFO',
                'formatter': 'normal',
                'stream': 'ext://sys.stdout',
              }, 
            'file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                'formatter': 'detailed',
                'filename': log_file_path, 
            }
        },
        'formatters': {
            'detailed': {
                'format': ("[%(asctime)s] [%(levelname)s] [%(module)s] " 
            "[%(funcName)s] [%(lineno)s]: %(message)s"),
            },
            'normal': {
                 'format': "[%(asctime)s] [%(levelname)s] [%(module)s]: %(message)s"
            }

        },
        'loggers': {
            '': {  # Root logger
                'handlers': ['console'],
                'level': 'DEBUG',
                'propagate': False,
            },
            'get_news': {
                'handlers': ['console', 'file'],
                'level': 'INFO',
                'propagate': False,
            },
            'ingest_news': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
            'get_np_news': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
            'sentiment_analysis': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
            'sentiment_dashboard': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
            'translate': {
                'handlers': ['console', 'file'],
                'level': 'DEBUG',
                'propagate': False
            },
        },
    })