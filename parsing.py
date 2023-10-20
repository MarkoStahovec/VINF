import csv
import glob
import re

from tqdm import tqdm
from utils import *


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


"""
def parse():
    # TODO: change csv to a different format
    # TODO: test pylucene with GPT
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
"""


def parse():
    list_of_files = glob.glob(f'{OUTPUT_FOLDER}*.txt')
    latest_file = max(list_of_files, key=os.path.getctime)

    with open(latest_file, 'r', encoding='utf-8') as f:
        content = f.read()
        pages = content.split(PAGE_DELIMITER)

    with open(OUTPUT_TSV, 'w', newline='', encoding='utf-8') as tsvfile:
        # Add header to TSV
        tsvfile.write("Artist\tSong_Name\tFeaturing\tAlbum_Name\tYear\tLyrics\n")

        for page in tqdm(pages, desc="Parsing pages", unit="page"):
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

                # Removing or replacing any literal tab characters in the content
                row_data = [artist, song_name, featuring, album_name, year, lyrics]
                sanitized_data = [data.replace('\t', ' ').replace('\n', ' ') for data in
                                  row_data]

                tsvfile.write('\t'.join(sanitized_data) + "\n")

    return
