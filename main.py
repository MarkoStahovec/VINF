import csv
import glob
import os
from datetime import datetime
import requests
import time
import re

BASE_DOMAIN = "www.azlyrics.com"
BASE_URL = "https://www.azlyrics.com/"
ROBOTS_URL = BASE_URL + "robots.txt"
USER_AGENT = "SCHOOL_PROJECT_CRAWLER: (xstahovec@stuba.sk)"
HEADERS = {"User-Agent": USER_AGENT}
OUTPUT_FOLDER = "./output/"
OUTPUT_CSV = "parsed_data.csv"

PAGE_DELIMITER = "#####PAGE_BREAK#####"
MAX_PAGES = 5
TIMEOUT = 5


def can_crawl(url):
    response = requests.get(ROBOTS_URL)
    lines = response.text.split("\n")

    disallowed_paths = []
    allowed = True
    user_agent_all = False

    for line in lines:
        if "User-agent: *" in line:
            user_agent_all = True
        elif user_agent_all and "User-agent:" in line:  # another user-agent, stop parsing
            break
        elif user_agent_all and "Disallow:" in line:
            path = line.split(":")[1].strip()
            disallowed_paths.append(path)
        elif user_agent_all and "Allow:" in line:
            path = line.split(":")[1].strip()
            if path in url:
                allowed = True

    for path in disallowed_paths:
        if path in url:
            allowed = False

    return allowed


def get_links_from_page(response):
    links = re.findall('<a[^>]* href="([^"]*)"', response.text)

    # resolve relative URLs and URLs starting with //
    resolved_links = []
    for link in links:

        # skip all links that lead elsewhere than to our domain
        if BASE_DOMAIN not in link:
            continue

        # skip all links that may not end up with html as those are not useful
        if not link.endswith(".html"):
            continue

        if link.startswith("//"):
            resolved_link = "https:" + link
        elif link.startswith("http"):
            resolved_link = link
        else:
            resolved_link = BASE_URL + link
        resolved_links.append(resolved_link)

    return resolved_links


# TODO: very primitive filtering of lyrics pages as it relies on a strict structure of hyperlinks
def should_extract_html(url):
    # Split the URL at ".com"
    parts = url.split('.com', 1)
    if len(parts) != 2:
        return False
    # Count the slashes in the part after ".com"
    return parts[1].count('/') >= 3


def save_to_file(filename, data):
    directory = os.path.dirname(filename)  # Extract directory path from filename

    if not os.path.exists(directory):  # If directory doesn't exist
        os.makedirs(directory)  # Create the directory

    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(data + '\n')
            f.write(PAGE_DELIMITER + '\n')  # Add the delimiter after each page
    except Exception as e:
        print(f"Error writing to file: {e}")


def extract_bold_text(content):
    return re.findall(r'<b[^>]*>(.*?)</b>', content)


# TODO: might need to be incorporated somehow
def extract_entities_from_url(url):
    # Splits the URL by slashes and filters out any part that contains a dot (likely a domain or filename).
    return [part for part in url.split('/') if part and '.' not in part]


def extract_lyrics(page):
    # TODO: weak lyrics extraction algorithm
    # Extract content between the two comments and hope for the best
    match = re.search(
        r'<!-- Usage of azlyrics\.com content by any third-party lyrics provider is prohibited by our '
        r'licensing agreement\. Sorry about that\. -->(.*?)<!-- MxM banner -->',
        page, re.DOTALL)
    if match:
        content = match.group(1)

        # Remove all HTML tags
        clean_content = re.sub(r'<.*?>', '', content)

        return clean_content
    else:
        return ""


def extract_artist(string):
    return string.replace(" Lyrics", "")


def extract_features(page):
    pattern = r'<span class="feat">\((.*?)\)</span>'
    matches = re.search(pattern, page)

    if matches:
        return matches.group(1)
    else:
        return ""


def extract_year(page):
    pattern = r'<div class="songinalbum_title">.*?\((\d{4})\)'
    matches = re.search(pattern, page)

    if matches:
        return matches.group(1)
    else:
        return ""


def parse():
    list_of_files = glob.glob(f'{OUTPUT_FOLDER}*.txt')
    latest_file = max(list_of_files, key=os.path.getctime)

    with open(latest_file, 'r', encoding='utf-8') as f:
        content = f.read()
        pages = content.split(PAGE_DELIMITER)

    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        csvwriter = csv.writer(csvfile)

        # Add header to CSV
        csvwriter.writerow(['Artist', 'Song_Name', 'Featuring', 'Album_Name', 'Year', 'Lyrics'])

        for page in pages:
            bold_texts = extract_bold_text(page)
            # if there are not bold texts, we are probably not on a lyrics page
            # this can be done more robustly by more regexes to find these entities, however, it might be unnecessary
            if bold_texts:
                artist = extract_artist(bold_texts[0])
                song_name = bold_texts[1].strip('"').replace('"', '')
                featuring = extract_features(page)
                album_name = bold_texts[2].strip('"').replace('"', '')
                year = extract_year(page)
                lyrics = extract_lyrics(page)

                csvwriter.writerow([artist, song_name, featuring, album_name, year, lyrics])

    return


def crawl():
    # Create a new file for this crawl session
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')  # current date and time
    filename = f"./output/crawled_data_{timestamp}.txt"
    if os.path.exists(filename):  # Ensure the file doesn't exist before we create it
        os.remove(filename)

    queue = [BASE_URL]
    queue.append('https://www.azlyrics.com/lyrics/snohaalegra/neonpeach.html')
    queue.append('https://www.azlyrics.com/lyrics/blackeyedpeas/myhumps.html')
    crawled = []

    while queue and len(crawled) < MAX_PAGES:
        url = queue.pop(0)
        if url not in crawled:
            if can_crawl(url):
                print(f"Crawling: {url}")
                response = requests.get(url, headers=HEADERS)
                content = response.content  # This gives you raw bytes
                text = content.decode('utf-8')

                # TODO: enable this for final version
                # if should_extract_html(url):
                #    save_to_file(filename, text)

                save_to_file(filename, text)
                links = get_links_from_page(response)
                queue.extend(links)
                crawled.append(url)
                time.sleep(TIMEOUT)  # Sleep for 5 sec before next request

    print(f"Finished Crawling! Total pages crawled: {len(crawled)}")


if __name__ == "__main__":
    # TODO: make timed crawling process with a txt file that stores
    #  current queue and a txt file with alredy crawled pages

    # TODO: decide tokenizer/lemmatization/stemming
    # TODO: create public github repo
    # crawl()
    parse()
