import glob
import random

import requests
import re
import time

from utils import *
from datetime import datetime


class CaptchaDetectedException(Exception):
    pass


# defines set of rules whether a page can be crawled parsing robots.txt file
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
    # Extract all the albums with their years
    album_patterns = [(m.start(), m.end(), m.group(1)) for m in
                      re.finditer(r'<div id="\d+" class="album">album:.*\((\d{4})\)<', response.text)]

    # A list to store all valid links
    valid_links = []

    base_name = os.path.basename(response.url)  # Get the last part of the URL,
    artist_name = os.path.splitext(base_name)[0]

    # Iterate through each match (album)
    for i, (start, end, year) in enumerate(album_patterns):
        year = int(year)

        # if it's a valid year
        if year >= 2022:
            # find the position of the current album in the response text
            current_position = start

            # find the position of the next album (if it exists), otherwise look for the "other songs" pattern or the end of the text
            if i + 1 < len(album_patterns):
                next_position = album_patterns[i + 1][0]
            else:
                # if it's the last album, then extract until the "<div class="album"><b>other songs:</b></div>" pattern or the end of the text
                other_songs_pos = response.text.find('<div class="album"><b>other songs:</b></div>', end)
                next_position = other_songs_pos if other_songs_pos != -1 else len(response.text)

            # extract all links between these two positions
            segment = response.text[current_position:next_position]
            links = re.findall(r'<a[^>]* href="([^"]*)"', segment)
            for link in links:
                if not link.endswith(".html"):
                    continue
                if artist_name not in link:
                    continue

                if link.startswith("//"):
                    resolved_link = "https:" + link
                elif link.startswith("http"):
                    resolved_link = link
                else:
                    resolved_link = BASE_URL + link
                valid_links.append(resolved_link)

    # extract links from "btn btn-menu" pattern
    btn_menu_links = re.findall(r'<a class="btn btn-menu" href="([^"]+)">', response.text)
    for link in btn_menu_links:
        if not link.endswith(".html"):
            continue

        # TODO: might have to delete this:
        if artist_name not in link:
            continue
        if link in valid_links:
            continue

        if link.startswith("//"):
            resolved_link = "https:" + link
        elif link.startswith("http"):
            resolved_link = link
        else:
            resolved_link = BASE_URL + link
        valid_links.append(resolved_link)

    # extract links that lead to artists profiles
    last_pattern_links = re.findall('<a href="([a-z]/[^/]+\.html)">', response.text)
    for link in last_pattern_links:
        if not link.endswith(".html"):
            continue
        if link in valid_links:
            continue

        if link.startswith("//"):
            resolved_link = "https:" + link
        elif link.startswith("http"):
            resolved_link = link
        else:
            resolved_link = BASE_URL + link
        valid_links.append(resolved_link)

    return valid_links


# TODO: very primitive filtering of lyrics pages as it relies on a strict structure of hyperlinks
def should_extract_html(url):
    parts = url.split('.com', 1)
    if len(parts) != 2:
        return False

    return parts[1].count('/') >= 3


def is_captcha_page(content):
    lowercased_content = content.lower()
    for keyword in ["captcha", "recaptcha", "challenge", "captch"]:
        if keyword in lowercased_content:
            return True
    return False


def get_random_user_agent():
    return random.choice(USER_AGENTS)


def crawl():
    while True:
        try:
            hours = int(input("[INPUT] - Enter a number of hours to crawl: "))
            # hours = 1
            if 1 <= hours <= 24:
                break
        except ValueError:
            print("")

    start_time = time.time()
    duration = hours * 60 * 60

    # check if the user wants to load from checkpoint
    # the method loads the newest txt file and continues adding to that, which relies heavily on the fact that no txt
    # files are added to the folder
    choice = input("[INPUT] - Do you want to load the last checkpoint? [y/n]: ")
    # choice = "y"
    if choice.lower().replace(" ", "") == "y":
        queue, crawled = load_state()
        list_of_files = glob.glob(f'{OUTPUT_FOLDER}*.txt')
        latest_file = max(list_of_files, key=os.path.getctime)
        filename = latest_file
    else:
        queue = [BASE_URL]
        crawled = []
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        filename = f"./output/crawled_data_{timestamp}.txt"
        if os.path.exists(filename):  # ensure the file doesn't exist before we create it
            os.remove(filename)

    batch = []  # holds a batch of pages so that we save by batches, not after every page

    try:
        while queue and len(crawled) < MAX_PAGES and time.time() - start_time < duration:
            url = queue.pop()
            if url not in crawled:
                if can_crawl(url):
                    custom_crawl_print(f"[INFO] - Crawling: {url}", queue, crawled)

                    headers = {
                        "User-Agent": get_random_user_agent(),
                        "From": "xstahovec@stuba.sk"
                    }
                    response = requests.get(url, headers=headers)

                    # special treatment for special cases
                    if response.status_code in [403, 429]:
                        custom_crawl_print(f"[WARNING] - Received status {response.status_code}. Sleeping for {LONG_TIMEOUT}s before next request.", queue, crawled)
                        queue.insert(0, url)
                        # queue.append(url)  # adding url to the end of the queue so we can try the link later
                        raise CaptchaDetectedException("Captcha detected")

                    elif response.status_code in [401, 404, 406, 409, 411]:
                        custom_crawl_print(
                            f"[WARNING] - Received status {response.status_code}. Sleeping for {TIMEOUT}s before next request.",
                            queue, crawled)
                        time.sleep(TIMEOUT)
                        continue

                    content = response.content  # raw bytes
                    text = content.decode('utf-8')

                    if is_captcha_page(text):
                        custom_crawl_print(
                            f"[WARNING] - Captcha detected. Sleeping for {LONG_TIMEOUT}s before next request.", queue,
                            crawled)
                        queue.insert(0, url)
                        # queue.append(url)  # re-add the URL to the queue to retry later
                        time.sleep(LONG_TIMEOUT)
                        continue

                    # TODO: enable this for final version
                    if should_extract_html(url):
                        custom_crawl_print(f"[INFO] - Saving content from: {url}", queue, crawled)
                        batch.append(text)

                    # genius logic to return only those links that are not in queue nor in crawled
                    # current url may be added to queue anyway, but it is not let through in the if url not in crawled
                    links = set(get_links_from_page(response)) - set(queue) - set(crawled)
                    queue.extend(links)
                    random.shuffle(queue)
                    crawled.append(url)

                    if len(batch) == BATCH_SIZE:
                        combined_batch = PAGE_DELIMITER.join(batch)
                        save_to_file(filename, combined_batch)  # save the batch of raw HTML
                        save_state(queue, crawled)  # save the state of queue and already crawled pages
                        batch.clear()  # empty out the batch
                        custom_crawl_print(f"[INFO] - Saving current batch and writing to file successfully.", queue, crawled)

                    random_delay = random.uniform(0, 5)
                    sleep_duration = TIMEOUT + random_delay

                    time.sleep(sleep_duration)

    except CaptchaDetectedException as e:
        custom_crawl_print(f"[CAPTCHA] - Captcha was detected: {e}", queue, crawled)
    except Exception as e:
        custom_crawl_print(f"[ERROR] - An error occurred while crawling: {e}", queue, crawled)

    finally:
        combined_batch = PAGE_DELIMITER.join(batch)
        save_to_file(filename, combined_batch)  # save the batch of raw HTML
        save_state(queue, crawled)  # save the state of queue and already crawled pages

    custom_crawl_print(f"[INFO] - Finished crawling, total pages crawled: {len(crawled)}", queue, crawled)


"""
def crawl():
    # Create a new file for this crawl session
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')  # current date and time
    filename = f"./output/crawled_data_{timestamp}.txt"
    if os.path.exists(filename):  # Ensure the file doesn't exist before we create it
        os.remove(filename)

    # queue = [BASE_URL]
    queue = []
    queue.append('https://www.azlyrics.com/lyrics/snohaalegra/neonpeach.html')
    queue.append('https://www.azlyrics.com/lyrics/blackeyedpeas/myhumps.html')
    crawled = []

    while queue and len(crawled) < MAX_PAGES:
        url = queue.pop(0)
        if url not in crawled:
            if can_crawl(url):
                print(f"[INFO] - Crawling: {url}")
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

    print(f"[INFO] - Finished crawling, total pages crawled: {len(crawled)}")
"""
