import logging

user_agent = "wf-dump-scripts by User:So9q, see Github for code"
BASE_URL: str = "https://www.wikifunctions.org/wiki"
BASE_API_URL: str = "https://www.wikifunctions.org/w/api.php"
loglevel = logging.INFO
httpx_loglevel = logging.WARNING
logformat = "%(asctime)s [%(levelname)s] %(message)s"
output_file_prefix = "output/wikitable-z8-stats"
log_progress_interval = 1000

# For testing: stop after this many functions
MAX_FUNCTIONS: int = 800
