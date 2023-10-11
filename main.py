from parsing import parse
from crawling import crawl


if __name__ == "__main__":
    # TODO: make timed crawling process with a txt file that stores
    #  current queue and a txt file with alredy crawled pages

    # TODO: decide tokenizer/lemmatization/stemming
    crawl()
    parse()
