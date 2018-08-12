from hdbcli import dbapi
import hana_credentials
from sqlalchemy import create_engine
from preprocess_tweets import preprocess


import json
from nltk.corpus import stopwords
import string
from dateutil.parser import parse
punctuation = list(string.punctuation)
stop = stopwords.words('english') + punctuation + ['rt', 'via', '...', 'RT','…']

#takes json format twitter one by one...given by input parameter 'data' formats it and insert into hana table named tweets

def cleanText(text):
    text_list=[term for term in preprocess(text) if term not in stop and not term.startswith(('#', '@', 'http'))]
    return " ".join(text_list)

def extractFields(data):
    tweet = json.loads(data) # load it as Python dict
    #print(json.dumps(tweet, indent=4)) # pretty-print
    username=tweet['user']['screen_name']
    
    text=cleanText(tweet['text'])
    
    created_at= parse(tweet['created_at'])
    
    #hashtags
    hashtags=''
    for hashtag in tweet['entities']['hashtags']:
        hashtags+=hashtag['text']+','
    hashtags=hashtags[:-1]#removing last comma
    hashtags= hashtags
    
    return username,text,created_at,hashtags


def insertIntoTable(data):
	username,text,created_at,hashtags=extractFields(data)

	conn = dbapi.connect(
    address="10.76.179.172",#"lddbha5.wdf.sap.corp", 
    port=31015, 
    user=hana_credentials.USER, 
    password=hana_credentials.PASSWORD
	)

	engine = create_engine(hana_credentials.ENGINE)

	cursor = conn.cursor() 

	stmt = 'insert into twitter (USERNAME, CREATED_AT, TEXT, HASHTAGS) values (?,?,?,?)'
	cursor.execute(stmt, (username,created_at,text,hashtags))
