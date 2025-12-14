import logging

user_agent = "wf-dump-scripts by User:So9q, see Github for code"
BASE_URL: str = "https://www.wikifunctions.org/wiki"
BASE_API_URL: str = "https://www.wikifunctions.org/w/api.php"
loglevel = logging.DEBUG
httpx_loglevel = logging.WARNING
logformat = "%(asctime)s [%(levelname)s] %(message)s"

# For testing: stop after this many functions
MAX_FUNCTIONS: int = 75
