import pandas as pd
import profanity_ckeck
import requets
import validators
import time
#Loading kym.json file as a dataframe object
df = pd.read_json('../Json_files/kym.json')

#Remove JSON object where the key category is different to Meme
df = df.drop(df[df.category != "Meme"].index).reset_index(drop=True)

#Remove the memes where the status is equal to DEADPOOL
index_to_drop = []
for index,element in enumerate(df.details):
    if(element['status'] == 'deadpool'):
        index_to_drop.append(index)
df = df.drop(labels = index_to_drop,axis = 0).reset_index(drop=True)


#Check the title of the memes with the value UNLISTED and SUBMISSION, and remove them if their title contains profanity or offensive languages.
index_to_drop = []
for index,element in enumerate(df.title):
    if(profanity_check.predict_prob([element])[0]>0.5):
        index_to_drop.append(index)
df = df.drop(labels = index_to_drop,axis = 0).reset_index(drop=True)


#Check the validity of the url of the memes and replace them if they are broken or nonexistent
url_value = []
for index,element in enumerate(df.url):
    if(validators.url(element)):
        print(element)
        if(requests.get(element).status_code!=200):
            url_value.append("link broken")
        else:
            url_value.append(element)
    else:
        url_value.append("no link")
    time.sleep(1)
df["url"] = url_value

#Remove the column id 
df.drop('ld', inplace=True, axis=1)

#Remove the column detail
df.drop('details', inplace=True, axis=1)

#Remove the columns siblings
df.drop('siblings', inplace=True, axis=1)

js = df.to_json('../Json_files/kym_cleaned.json',orient = 'index')




