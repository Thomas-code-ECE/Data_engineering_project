import pandas as pd 
from pattern.text.en import singularize 
from keybert import KeyBERT

#Loading kym.json file as a dataframe object
df = pd.read_json('json_files/kym_cleaned.json')

#Remove the column meta and keep the column: width, height and description
width_value = []
height_value = []
description_value = []
for index,element in enumerate(df.meta):
    width_value.append(element["og:image:width"])
    height_value.append(element["og:image:height"])
    if("description" in element):
        description_value.append(element["description"])
    else:
        description_value.append("unknown")
df["widht"] = width_value
df["height"] = height_value
df["description"] = description_value
df = df.drop(labels = "meta" , axis = 1)

# Converting the date of the last update and the posted date of the meme from UNIX timestamp to classic date 
df['last_update_source'] = pd.to_datetime(df['last_update_source'],unit='s').dt.normalize()
df['added'] = pd.to_datetime(df['added'],unit='s').dt.normalize()

#Remove the meme posted before November 25, 2007 and after 2022
df = df.drop(df[(df.added < '2007-11-25') & (df.added > '2022-01-01') & (df.added < '2007-11-25') & (df.added > '2022-01-01')].index)

#Remove unicode characters from description
description_value = []
for index,element in enumerate(df.description):
    description_value.append(element.encode("ascii","ignore").decode())
df["description"] = description_value

#Set the tag words in lower case and in singular form. Then, remove the redundant tag words related to one meme
tags_meme = []
for index,element in enumerate(df.tags):
    tags_meme.append(list(dict.fromkeys([singularize(tag.lower()) for tag in element])))
df["tags"] = tags_meme

#Transform the description text into a cluster of keywords. --> keywords array
#Transform all keywords into singular and lower form, then remove the redundant keywords. --> array_lower_case_singular
kw_model = KeyBERT()
descriptions_keywords = []
for index,element in enumerate(df.description):
    keywords = [keyword[0] for keyword in kw_model.extract_keywords(element, keyphrase_ngram_range=(1, 1), stop_words=None)]
    array_lower_case_singular = list(dict.fromkeys([singularize(keyword.lower()) for keyword in keywords]))
    descriptions_keywords.append(array_lower_case_singular)
df["description"] = descriptions_keywords

# Transform the links from the column template_image_url. Remove the first part "https://i.kym-cdn.com/entries/icons/original/000/" which is common to all memes
template_url_meme = []
for index,element in enumerate(df.template_image_url):
    template_url_meme.append(element.split('original/000/')[1])
df["template_image_url"] = template_url_meme 

#Transform the links from the column url. Remove the first part "https://knowyourmeme.com/memes/" which is common to all memes
url_meme = []
for index,element in enumerate(df.url):
    url_meme.append(element.split('/memes/')[1])
df["url"] = url_meme 

#Transform the column children. Either the key gets children, create an array with the url where each link has been modified. In the same way as before, we delete the first part of the link which is redundant between the memes "https://knowyourmeme.com/memes/"
df.loc[df['children'].isnull(),['children']] = df.loc[df['children'].isnull(),'children'].apply(lambda x: [])
children_meme = []
for index,element in enumerate(df.children):
    if(len(element) != 0):
        children_meme.append([children.split('/')[-1] for children in element])
    else:
        children_meme.append([])
df["children"] = children_meme

#Transform the column parents. Either the key gets parent, create an array with the url where each link has been modified. In the same way as before, we delete the first part of the link which is redundant between the memes "https://knowyourmeme.com/memes/"
df.loc[df['parent'].notnull(),['parent']] = df.loc[df['parent'].notnull(),'parent'].apply(lambda x: x.split('/')[-1])
df.loc[df['parent'].isnull(),['parent']] = df.loc[df['parent'].isnull(),'parent'].apply(lambda x: 'no parent')

#Keep only the website of the references
web_references_meme = []
for index,element in enumerate(df.additional_references):
    web_references_meme.append(list(element.keys()))
df["additional_references"] = web_references_meme

df.loc[df['search_keywords'].isnull(),['search_keywords']] = df.loc[df['search_keywords'].isnull(),'search_keywords'].apply(lambda x: [])
search_keywords_meme= []
for index,element in enumerate(df.search_keywords):
    search_keywords_meme.append([''.join(filter(str.isalnum,keywords)) for keywords in element])
df["search_keywords"] = search_keywords_meme

js = df.to_json('json_files/kym_transformed.json',orient = 'index')