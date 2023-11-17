import pandas as pd
import validators

df = pd.read_csv('wiki_data.csv', sep="\t", error_bad_lines=False)
strings_to_replace = ["{{", "{{URL", "{{url", "\|",
                      "\[\[", "\]\]", "<!--.*?-->",
                      "URL|", "start date", "}}", "</ref>", "<ref>",
                      "url", "cite web", "Cite book", "cite book",
                      "Cite web"]

for s in strings_to_replace:
    df = df.replace(s, '', regex=True)


strings_to_replace = ["&ndash"]

for s in strings_to_replace:
    df = df.replace(s, '-', regex=True)


def check_length(value):
    if isinstance(value, str) and len(value) > 50:
        return None
    else:
        return value


df = df.applymap(check_length)


def validate_url(url):
    if validators.url(url):
        return url
    else:
        return None


# df['website'] = df['website'].apply(validate_url)

print(df)
df.to_csv("wiki_data_cleansed.tsv", sep="\t", index=False)


"""
            ----------------------------------
"""


parsed_data = pd.read_csv('parsed_data.tsv', delimiter='\t')
wiki_data = pd.read_csv('wiki_data_cleansed.tsv', delimiter='\t')
wiki_data_subset = wiki_data[wiki_data['name'].isin(parsed_data['Artist']) | wiki_data['title'].isin(parsed_data['Artist'])]
merged_data_title = pd.merge(parsed_data, wiki_data_subset, left_on='Artist', right_on='title', how='left')
unmatched_artists = merged_data_title[merged_data_title['title'].isna()]['Artist']
unmatched_data = parsed_data[parsed_data['Artist'].isin(unmatched_artists)]
merged_data_name = pd.merge(unmatched_data, wiki_data_subset, left_on='Artist', right_on='name', how='left')
final_merged_data = pd.concat([merged_data_title[~merged_data_title['Artist'].isin(unmatched_artists)], merged_data_name])
# final_merged_data = final_merged_data.set_index('Artist').reindex(index=parsed_data['Artist']).reset_index()
final_merged_data = final_merged_data[["Artist", "Song_Name", "Featuring", "Album_Name", "Year", "Lyrics", "years", "website", "origin"]]
final_merged_data['years'] = final_merged_data['years'].str.replace('–', '-')
final_merged_data.to_csv('final_merged_data.tsv', sep='\t', index=False, header=True, encoding='utf-8')
print(final_merged_data)

