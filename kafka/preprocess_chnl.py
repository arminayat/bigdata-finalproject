# -*- coding: utf-8 -*-
"""
Created on Thu Jun 24 19:10:58 2021

@author: yaram
"""

from kafka import KafkaConsumer, KafkaProducer

import json
from json import dumps, loads

import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from hazm import *
import string

from time import sleep
import os



#TFIDF 
corpus = []
produce_idx = 0
keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت', 'روحانی', 'انتخابات', 'دلار', 'طلا', 'کرونا', 'تورم', 'دانشگاه']
covid_keywords = ['کوید19', 'کووید19', 'کوید', 'کووید19']



########################### Load from files #############################


# online corpus for TFIDF
corpus_path = './data/corpus.txt'
if os.path.isfile(corpus_path):
    f = open(corpus_path, 'r', encoding="utf8")
    corpus = f.read()
    corpus = corpus.splitlines()
    f.close()

# Stopwords
f = open('./data/hazm_stopwords.txt', 'r', encoding="utf8")
stop_words = f.read()
stop_words = stop_words.splitlines()
f.close()

# Verbs (Lemmatized)
f = open('./data/hazm_verbs.txt', 'r', encoding="utf8")
verbs = f.read()
verbs = verbs.splitlines()
f.close()



######################## Helper Methods ################################


def sort_coo(coo_matrix):
    tuples = zip(coo_matrix.col, coo_matrix.data)
    return sorted(tuples, key=lambda x: (x[1], x[0]), reverse=True)



def extract_topn_from_vector(feature_names, sorted_items, topn=10):
    """get the feature names and tf-idf score of top n items"""
    
    #use only topn items from vector
    sorted_items = sorted_items[:topn]

    score_vals = []
    feature_vals = []
    
    # word index and corresponding tf-idf score
    for idx, score in sorted_items:
        
        #keep track of feature name and its corresponding score
        score_vals.append(round(score, 3))
        feature_vals.append(feature_names[idx])

    #create a tuples of feature,score
    #results = zip(feature_vals,score_vals)
    results= {}
    for idx in range(len(feature_vals)):
        results[feature_vals[idx]]=score_vals[idx]
    
    return results



######################## Preprocessing Functions ########################


def extract_hashtags_urls(text):
    
    # Extract Hashtags
    tags = re.findall("#(\w+)", text)

    # Extract URLS         # Optional http(s) and www                          no persian letters     and words including .           and words including / and .
    url_groups = re.findall('(http:\/\/www\.|https:\/\/www\.|http:\/\/|https:\/\/)?(?![\u0600-\u06FF\s])+([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?' , text)
    urls = []
    for url in url_groups:
         urls.append(''.join(url))
         
    return tags, urls


def preprocess_text(text):
    global stop_words, verbs
    
    text = text.replace('_', ' ') # split hashtag words 
    
    text = text.translate(str.maketrans('', '', string.punctuation + "؟!.،,?:؛»«")) # Remove punctuation
    normalizer = Normalizer() # use halfspaces
    text = normalizer.normalize(text)
    stemmer = Stemmer()
    text = stemmer.stem(text) # Make plurals singular
    lemmatizer = Lemmatizer()
    text = lemmatizer.lemmatize(text) # Verbs
    
    tokenizer = WordTokenizer(replace_links=True, replace_IDs=True, replace_numbers=True, separate_emoji=True, replace_hashtags=False, replace_emails=True)
    text = word_tokenize(text) # Split words
    #                                      remove stopwords      remove verbs                   remove numbers and english words
    text = [word for word in text if (word not in stop_words) and (word not in verbs) and not re.search(r'[a-zA-Z\u06F0-\u06F90-9]', word)]

    text = ' '.join(text)
    return text


def keyword_extraction(text, produce_idx):
    global keywords, covid_keywords, corpus
    
    # Extract Predefined keys
    extracted_keys = []
    for k in keywords:
        if k in text:
            extracted_keys.append(k)
    for k in covid_keywords:
        if k in text:
            extracted_keys.append('کوید19')
            break
            
    # Only keep last 50,000 documents in online corpus
    if len(corpus) == 50000: 
        print('corpus maximum size reached')
        corpus.pop(0)
    corpus.append(text)
    
    if len(corpus) > 100: # First, build a descent corpus then start saving tweets
        
        # Update corpus and IDF
        cv = CountVectorizer(max_features=20000, lowercase=False) # Only keep 20000 top words
        word_count_vector=cv.fit_transform(corpus)
        tfidf_transformer=TfidfTransformer(smooth_idf=True,use_idf=True)
        tfidf_transformer.fit(word_count_vector) # update IDF
        
        # Get keywords by tfidf
        tf_idf_vector=tfidf_transformer.transform(cv.transform([text]))
        sorted_items=sort_coo(tf_idf_vector.tocoo())
        keywords=extract_topn_from_vector(cv.get_feature_names(),sorted_items,10)
        for k in keywords:
            if float(keywords[k]) > 0.5: # Threshold
                extracted_keys.append(k)
                    
    return extracted_keys
    
    

########################## Define Kafka clients #########################


# Produce clean tweets
producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'),
                         api_version=(0,10))

# Consume dirty tweets
consumer = KafkaConsumer(
    "dirty_tweets",
     bootstrap_servers=['localhost:29092'],
     auto_offset_reset= 'earliest', # 'earliest', # Start from last consumed, #'latest' start from last produce
     enable_auto_commit=True,
     auto_commit_interval_ms = 1000, #ms # Ok. cuz our messages come every 5 seconds
     group_id='twitter',
     value_deserializer=lambda x: loads(x.decode('utf-8')),
     api_version=(0,10))



######################## Consume, Clean, Produce #######################

# for message in consumer:
#     print(message.value)
#     print(message.key)

try:
    for message in consumer:
        #print(message.value)
        #print(message.key)
        
        json_ = message.value
        # If tweet is longer than 140 characters, it isnt stored fully in Text field
        text = json_["text"] if not json_["truncated"] else json_["extended_tweet"]["full_text"]
    
        # preprocessing for keyword extraction
        text2 = preprocess_text(text) 
        
        # Data to add
        json_['text_k'] = text         
        json_["hashtags_k"], json_["urls_k"] = extract_hashtags_urls(text) # Find tags and urls
        json_["keywords_k"] = keyword_extraction(text2, produce_idx)
            
        print('text: \t', json_['text_k'])
        print('tags: \t', json_["hashtags_k"])
        print('urls: \t', json_["urls_k"])
        print('keys: \t', json_["keywords_k"])
                
        print("*************************")
        
        producer.send('clean_tweets', value=json_)
    
except KeyboardInterrupt:
    print('Interrupting! Saving Corpus...')
    f = open(corpus_path, 'w+', encoding="utf8")
    for twit in corpus:
        f.write(twit + '\n')
    f.close()

