import requests
import re
import time

from utils import *
from datetime import datetime


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


"""
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
"""


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

        # If it's a valid year
        if year >= 2022:
            # Find the position of the current album in the response text
            current_position = start

            # Find the position of the next album (if it exists), otherwise look for the "other songs" pattern or the end of the text
            if i + 1 < len(album_patterns):
                next_position = album_patterns[i + 1][0]
            else:
                # If it's the last album, then extract until the "<div class="album"><b>other songs:</b></div>" pattern or the end of the text
                other_songs_pos = response.text.find('<div class="album"><b>other songs:</b></div>', end)
                next_position = other_songs_pos if other_songs_pos != -1 else len(response.text)

            # Extract all links between these two positions
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

    # Extract links from "btn btn-menu" pattern
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

    # Extract links from the last pattern
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
    # Split the URL at ".com"
    parts = url.split('.com', 1)
    if len(parts) != 2:
        return False
    # Count the slashes in the part after ".com"
    return parts[1].count('/') >= 3


def crawl():
    # Create a new file for this crawl session
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')  # current date and time
    filename = f"./output/crawled_data_{timestamp}.txt"
    if os.path.exists(filename):  # Ensure the file doesn't exist before we create it
        os.remove(filename)

    # queue = [BASE_URL]
    queue = []
    queue.append("https://www.azlyrics.com/b.html")
    # queue.append('https://www.azlyrics.com/lyrics/snohaalegra/neonpeach.html')
    # queue.append('https://www.azlyrics.com/lyrics/blackeyedpeas/myhumps.html')
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
