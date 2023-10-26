import os
import json

from datetime import datetime

BASE_DOMAIN = "www.azlyrics.com"
BASE_URL = "https://www.azlyrics.com/"
ROBOTS_URL = BASE_URL + "robots.txt"
USER_AGENT = "SCHOOL_PROJECT_CRAWLER: (xstahovec@stuba.sk)"
USER_AGENTS = [
    "SCHOOL_PROJECT_CRAWLER, STUBA.SK: (xstahovec@stuba.sk)",
    "SCHOOL_PROJECT_CRAWLER: (xstahovec@stuba.sk)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:55.0) Gecko/20100101 Firefox/55.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Mobile Safari/537.36"
]
HEADERS = {"User-Agent": USER_AGENT}
OUTPUT_FOLDER = "./output/"
OUTPUT_TSV = "parsed_data.tsv"
QUEUE_FILE = "./queue.json"
CRAWLED_FILE = "./crawled.json"

PAGE_DELIMITER = "\n#####PAGE_BREAK#####\n"
MAX_PAGES = 99999
TIMEOUT = 10
LONG_TIMEOUT = 60
BATCH_SIZE = 128


def custom_crawl_print(message, queue, crawled):
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {message} | Queue Length: {len(queue)} | Crawled: {len(crawled)}")


def custom_print(message):
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {message}")


def save_to_file(filename, data):
    directory = os.path.dirname(filename)  # Extract directory path from filename

    if not os.path.exists(directory):  # If directory doesn't exist
        os.makedirs(directory)  # Create the directory

    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(data)
    except Exception as e:
        print(f"[ERROR] - Error writing to file: {e}")


def save_state(queue, crawled):
    with open(QUEUE_FILE, "w") as qfile:
        json.dump(queue, qfile)

    with open(CRAWLED_FILE, "w") as cfile:
        json.dump(crawled, cfile)


def load_state():
    if os.path.exists(QUEUE_FILE) and os.path.exists(CRAWLED_FILE):
        with open(QUEUE_FILE, "r") as qfile:
            queue = json.load(qfile)

        with open(CRAWLED_FILE, "r") as cfile:
            crawled = json.load(cfile)

        return queue, crawled
    else:
        print("[ERROR] - There are no checkpoint files.")
        exit(2)
