import os

BASE_DOMAIN = "www.azlyrics.com"
BASE_URL = "https://www.azlyrics.com/"
ROBOTS_URL = BASE_URL + "robots.txt"
USER_AGENT = "SCHOOL_PROJECT_CRAWLER: (xstahovec@stuba.sk)"
HEADERS = {"User-Agent": USER_AGENT}
OUTPUT_FOLDER = "./output/"
OUTPUT_TSV = "parsed_data.tsv"

PAGE_DELIMITER = "#####PAGE_BREAK#####"
MAX_PAGES = 2
TIMEOUT = 5


def save_to_file(filename, data):
    directory = os.path.dirname(filename)  # Extract directory path from filename

    if not os.path.exists(directory):  # If directory doesn't exist
        os.makedirs(directory)  # Create the directory

    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(data + '\n')
            f.write(PAGE_DELIMITER + '\n')  # Add the delimiter after each page
    except Exception as e:
        print(f"[ERROR] - Error writing to file: {e}")
