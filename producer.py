# Producer

from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

from tweepy import Stream
from kafka import KafkaProducer

import json

import re
from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from hazm import *
import string

import os 
import time


#TFIDF 
corpus = []
produce_idx = 0
keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت', 'روحانی', 'انتخابات', 'دلار', 'طلا', 'کرونا', 'تورم', 'دانشگاه']
covid_keywords = ['کوید19', 'کووید19', 'کوید', 'کووید19']


########################### Load from files #############################


# online corpus for TFIDF
corpus_path = r'F:\UNI\99-2\Big Data\Projects\corpus.txt'
if os.path.isfile(corpus_path):
    f = open(corpus_path, 'r', encoding="utf8")
    corpus = f.read()
    corpus = corpus.splitlines()
    f.close()

# Stopwords
f = open(r'F:\UNI\99-2\Big Data\Projects\hazm_stopwords.txt', 'r', encoding="utf8")
stop_words = f.read()
stop_words = stop_words.splitlines()
f.close()

# Verbs (Lemmatized)
f = open(r'F:\UNI\99-2\Big Data\Projects\hazm_verbs.txt', 'r', encoding="utf8")
verbs = f.read()
verbs = verbs.splitlines()
f.close()



######################## Set Up Tweeter app ############################


access_token = '1086007829469618176-8CJCE0z1Gr0dm9EaseZbBrbqFfyMEA'          
access_token_secret =  '2e4emi6nPZ2gRoCAkzL9mfPARTbGq5qNcXkxXeif2pTw5' 
api_key =   'fAbcl4V3tYHOaBfpxzOodTiP1'
api_secret =  'lTU69KpsMGQKQ8pwLu9XMA7CEHT3Z57NqqynBzI9IkpQfXNhv3'

auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)



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
    
    

###################### Streamer and Producer ##########################


class stream_listener(StreamListener):
    
    
    def on_data(self, data):
        global produce_idx
        
        json_ = json.loads(data) # String to Dict
        #for attribute, value in json_.items():
        #    print(attribute)
        #    print(value)
        #    input("********")
        #input("*******************")
        
         # If tweet is longer than 140 characters, it isnt stored fully in Text field
        text = json_["text"] if not json_["truncated"] else json_["extended_tweet"]["full_text"]

        # preprocessing for keyword extraction
        text2 = preprocess_text(text) 
        #print(text2)
        
        # Data to produce
        saved_data = {}
        saved_data['text'] = text         
        saved_data['created_at'] = json_["created_at"]
        saved_data['timestamp_ms'] = json_['timestamp_ms']
        saved_data['id'] = json_['id']
        saved_data["hashtags"], saved_data["urls"] = extract_hashtags_urls(text) # Find tags and urls
        saved_data["keywords"] = keyword_extraction(text2, produce_idx)
        
        
        print(saved_data)
        
        #producer.send("basic", value=json.dumps(saved_data).encode('utf-8'))
        produce_idx += 1
        
        time.sleep(5)
        
        return True
    
    def on_error(self, status):
        print (status)
        
      

producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = stream_listener()

try:
    stream = Stream(auth, l)
    # Can't filter only on language
    # Can't retrieve more than 1% of tweets in 
    stream.filter(track = stop_words, languages=["fa"])

except KeyboardInterrupt:
    print('Interrupting! Saving Corpus...')
    f = open(corpus_path, 'w+', encoding="utf8")
    for twit in corpus:
        f.write(twit + '\n')
    f.close()

