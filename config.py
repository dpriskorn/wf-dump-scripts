import logging

user_agent = "wf-dump-scripts by User:So9q, see Github for code"
BASE_API_URL: str = "https://www.wikifunctions.org/w/api.php"
uselang: str = "en"
loglevel = logging.INFO
httpx_loglevel = logging.WARNING
logformat = "%(asctime)s [%(levelname)s] %(message)s"

# For testing: stop after this many functions
MAX_FUNCTIONS: int = 10
