import re

import lucene
import csv
import os

from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import MMapDirectory, NIOFSDirectory
from java.nio.file import Paths
from org.apache.lucene.search import IndexSearcher, FuzzyQuery
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.queryparser.classic import QueryParser

from org.apache.lucene.analysis.core import LowerCaseFilter, WhitespaceTokenizer
from org.apache.lucene.analysis.pattern import PatternReplaceFilter
from org.apache.pylucene.analysis import PythonAnalyzer
from java.util.regex import Pattern

from utils import *
from tqdm import tqdm


class CustomAnalyzer(PythonAnalyzer):

    def createComponents(self, fieldName):
        source = WhitespaceTokenizer()
        filter1 = LowerCaseFilter(source)

        # pattern to match special characters (excluding whitespace).
        pattern = Pattern.compile("[^a-zA-Z0-9\\s]")

        # PatternReplaceFilter to replace matched special characters with an empty string.
        filter2 = PatternReplaceFilter(filter1, pattern, "", True)

        return self.TokenStreamComponents(source, filter2)


def search_index(query_str, index_dir="index_directory"):
    directory = MMapDirectory(Paths.get(index_dir))
    reader = DirectoryReader.open(directory)
    searcher = IndexSearcher(reader)
    analyzer = CustomAnalyzer()

    # enforce phrase search
    if not (query_str.startswith('"') and query_str.endswith('"')):
        query_str = f'"{query_str}"'
    query = QueryParser("Lyrics", analyzer).parse(query_str)
    hits = searcher.search(query, 10).scoreDocs

    # fallback to individual word search
    if len(hits) < 10:
        query_str = query_str.strip('"')
        query = QueryParser("Lyrics", analyzer).parse(query_str)
        hits += searcher.search(query, 10 - len(hits)).scoreDocs

    # fallback to Fuzzy Search
    if len(hits) < 10:
        terms = query_str.split()  # splitting the query into individual terms for fuzzy search
        for term in terms:
            fuzzy_query = FuzzyQuery(Term("Lyrics", term), 2)  # fuzziness level set to 2
            fuzzy_hits = searcher.search(fuzzy_query, 10 - len(hits)).scoreDocs
            hits += fuzzy_hits

    results = []
    for hit in hits:
        hit_doc = searcher.doc(hit.doc)
        results.append({
            'Artist': hit_doc.get("Artist"),
            'Song_Name': hit_doc.get("Song_Name"),
            'Featuring': hit_doc.get("Featuring"),
            'Album_Name': hit_doc.get("Album_Name"),
            'Year': hit_doc.get("Year"),
            'Lyrics': hit_doc.get("Lyrics")
        })

    return results


def create_index(data, index_dir="index_directory"):
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)

    directory = MMapDirectory(Paths.get(index_dir))
    analyzer = CustomAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)  # forces creation of a new index always
    writer = IndexWriter(directory, config)

    # wrap the data with tqdm for a progress bar
    for row in tqdm(data, desc="Indexing", unit="doc"):
        doc = Document()
        doc.add(StringField("Artist", row['Artist'], Field.Store.YES))
        doc.add(StringField("Song_Name", row['Song_Name'], Field.Store.YES))
        doc.add(StringField("Featuring", row['Featuring'], Field.Store.YES))
        doc.add(StringField("Album_Name", row['Album_Name'], Field.Store.YES))
        doc.add(StringField("Year", row['Year'], Field.Store.YES))
        doc.add(TextField("Lyrics", row['Lyrics'], Field.Store.YES))  # TextField tokenizes the data

        writer.addDocument(doc)

    writer.commit()
    writer.close()

    custom_print("[INFO] - Index created successfully.")


def load_tsv(tsv_filename):
    with open(tsv_filename, 'r', encoding='utf-8') as tsvfile:
        reader = csv.DictReader(tsvfile, delimiter='\t')
        data = [row for row in reader]
    return data


# placeholder function for index settings
def settings(data):
    idx = input("\n\n[OPTION] - Create new index? [y/n]: ")
    # idx = "y"
    if idx.lower().replace(" ", "") == "y":
        create_index(data)


def sanitize_query(query):
    special_chars_pattern = re.compile('[^a-zA-Z0-9\\s]')
    # replace special characters with a space
    sanitized_query = special_chars_pattern.sub(' ', query)
    return sanitized_query


def index():
    # TODO: needs to be called only once
    lucene.initVM(vmargs=['-Djava.awt.headless=true', '-Xmx2g'])
    data = load_tsv(OUTPUT_TSV)

    print("\n\n-----------------------------------------------")
    print("\n\t* Shazam for lyrics\n\t* Author: Marko Stahovec\n")
    print("-----------------------------------------------")

    settings(data)
    while True:
        search_string = input("[INPUT] - Input searched phrase or press Enter to exit:  ")

        # exit on an empty string
        if not search_string.strip():
            custom_print("[INFO] - Exiting program.")
            exit(0)

        sanitized_search_string = sanitize_query(search_string)
        results = search_index(sanitized_search_string)
        custom_print(f"\n[INFO] - Printing results for: \"{sanitized_search_string}\".\n")

        for idx, song in enumerate(results, start=1):
            # featuring = f" (Featuring: {song['Featuring']})" if song['Featuring'] else ""
            featuring = f" ({song['Featuring']})" if song['Featuring'] else ""
            album = f"Album: {song['Album_Name']}" if song['Album_Name'] else ""
            year = f"Year: {song['Year']}" if song['Year'] else ""

            output = f"{idx}. {song['Artist']} - {song['Song_Name']}{featuring}"
            if album or year:
                additional_info = []
                if album:
                    additional_info.append(album)
                if year:
                    additional_info.append(year)
                output += f" ({', '.join(additional_info)})"
            print(output)

        print("\n")
