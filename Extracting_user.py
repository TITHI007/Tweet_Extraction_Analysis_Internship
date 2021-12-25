'''
Extracting Tweets and it's conversation of particular user using twitter api2.0
author:-TITHI PATEL
        ARCHI SHAH
'''

import datetime
import time
import sys
import requests
import os
from os import path
import json

sys.setrecursionlimit(3500)

search_url="https://api.twitter.com/2/tweets/search/all"

_bearerToken = ''
bearer_token = _bearerToken


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


headers = create_headers(bearer_token)


def connect_to_endpoint(url, headers, params):
    # connecting to endpoint and provide response in json format
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
        # it raises exception if any error occurs while connecting to endpoint
    return response.json()


def list_to_Json(Final_List, File_Name):
    # converts list of json response to json file
    with open("./data/{}/{}.json".format(File_Name,File_Name), 'a', encoding='utf8') as file:
        json.dump(Final_List, file, sort_keys=False, indent=4)


def list_append(i,json_response,tw):
    # appending list of json response in existing json file
    tweet = []
    # if file exist, it continue appending in same file else create file and dumps in it

    if path.isfile("./data/{}/{}_{}.json".format(i, i,tw)) is False:
        with open("./data/{}/{}_{}.json".format(i, i,tw), 'w', encoding='utf8') as js_file:
            tweet.append(json_response)
            json.dump(tweet, js_file, sort_keys=False, indent=4)
    else:
        with open("./data/{}/{}_{}.json".format(i, i,tw)) as js_file:
            tweet = json.load(js_file)
            tweet.append(json_response)
        with open("./data/{}/{}_{}.json".format(i, i,tw), 'w', encoding='utf8') as js_file:
            json.dump(tweet, js_file, sort_keys=False, indent=4)

def create_url(conversationid):
    # creating url of a particular conversation thread
    return "https://api.twitter.com/2/tweets/search/all?query=conversation_id:{}".format(conversationid)


def get_params():
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld

    start = datetime.datetime.strptime("2020-01-01T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").isoformat() + 'Z'
    end = datetime.datetime.strptime("2021-10-14T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ").isoformat() + 'Z'

    query_params = {
        'tweet.fields': "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
        'expansions': "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
        'start_time': start, 'end_time': end, 'max_results': 100, 'next_token': next_token}
    return query_params


def replies(Main_id):
    # It is recursive function for formating replies level wise

    l = []
    for inStance in Conversation_Tweet:
        if 'data' in inStance:
            for values in inStance['data']:
                if (values['referenced_tweets'][0]['id'] == Main_id):
                    # if the replied to id exist and is same as the main id provided while using function then function will repeat itself and form different level
                    # else will return the list of existing level

                    if values['public_metrics']['reply_count'] == 0:
                        v = {"author_id": values['author_id'], "conversation_id": values['conversation_id'],
                             "created_at": values['created_at'], "id": values['id'], "lang": values['lang'],
                             "possibly_sensitive": values['possibly_sensitive'],
                             "public_metrics": values['public_metrics'], "in_reply_to_status_id": values['id'],
                             "referenced_tweets": values['referenced_tweets'],
                             "reply_settings": values['reply_settings'], "source": values['source'],
                             "text": values['text']}
                        l.append(v)
                    else:
                        v = {"author_id": values['author_id'], "conversation_id": values['conversation_id'],
                             "created_at": values['created_at'], "id": values['id'], "lang": values['lang'],
                             "possibly_sensitive": values['possibly_sensitive'],
                             "public_metrics": values['public_metrics'], "in_reply_to_status_id": values['id'],
                             "referenced_tweets": values['referenced_tweets'],
                             "reply_settings": values['reply_settings'], "source": values['source'],
                             "text": values['text'], "replies": replies(values['id'])}
                        l.append(v)

    return l


lis_user=["ChariteBerlin","ViolaPriesemann","EckerleIsabella","Helmholtz_HZI","quarkswdr","bzga_de","alena_buyx","maithi_nk","UlrikeProtzer","CarstenWatzl","ZDDK_","ChanasitJonas","Flying__Doc","hendrikstreeck","CiesekSandra","drluebbers","evawolfangel","Sander_Lab","ethikrat","rki_de","WolfgangMueckst","BAG_OFSP_UFSP","ECMOKaragianni1","AlexanderKekule","NatalieGrams","lehr_thorsten","c_drosten","medwatch_de","PEI_Germany","KorinnaHennig","spektrum","spektrum","annetteless","TheBinderLab","BMG_Bund","SwissScience_TF","MrWissen2Go","rudi_anschober","RKI_fuer_Euch","bmsgpk","CorneliaBetsch","stohr_klaus","BrinkmannLab","ChristinaBerndt","alain_berset_buyx","Martin_Moder","RangaYogeshwar"]

for i in lis_user:
        if os.path.isdir("./data/{}".format(i)) is False:
            os.mkdir("./data/{}".format(i))

        next_token = None
        # it is a token to get another set of tweets

        t = True
        conversation_list = []
        # list of coversation id of the main tweet having reply

        start = datetime.datetime.strptime("2020-01-01T00:00:00Z","%Y-%m-%dT%H:%M:%SZ").isoformat() + 'Z'
        end = datetime.datetime.strptime("2021-09-15T00:00:00Z","%Y-%m-%dT%H:%M:%SZ").isoformat() + 'Z'

        print(i)
        while t:
            query_params = {'query': 'from:{} lang:de'.format(i),
                            # 'tweet.fields':['id','author_id','text','conversation_id','created_at','lang','context_annotations',
                            #                 'entities','geo','in_reply_to_user_id','non_public_metrics','organic_metrics',
                            #                 'possibly_sensitive','promoted_metrics','public_metrics','referenced_tweets',
                            #                 'reply_settings','source'],
                            'tweet.fields': "attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
                            'expansions': "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
                            'start_time': start, 'end_time': end, 'max_results': 100, 'next_token': next_token}
            try:
                json_response = connect_to_endpoint(search_url, headers, query_params)
                if 'data' in json_response:
                    # collects the conversation id

                    for j in json_response['data']:
                        conversation_list.append(j['conversation_id'])
            except:
                time.sleep(5)
                continue

            list_append(i, json_response,"Main_Tweets")
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
            # if there are more than 100 tweets it will have next token which helps us to collect following 100 tweets

            else:
                t = False
                break
            time.sleep(5)

        print(len(conversation_list))


        Conversation_Tweet = []
        # varible which collects json form of replies in list
        next_token = None

        n = 0
        for conv_id in conversation_list:
            # loop to get replies from coversation id
            conversationid = conv_id
            url = create_url(conversationid)
            t = True
            print("conversationid:{}".format(n))
            while t:
                params = get_params()
                try:
                    json_response = connect_to_endpoint(url, headers, params)
                except:
                    time.sleep(5)
                    continue

                list_append(i, json_response,"Conversation_Tweets")
                # Conversation_Tweet.append(json_response)

                if 'next_token' in json_response['meta']:
                    next_token = json_response.get('meta').get('next_token')
                else:
                    t = False
                    break
            n = n + 1

        # list_to_Json(Conversation_Tweet, "{}_Conversation_Tweets".format(i))

        # print(len(Conversation_Tweet))
        # ['referenced_tweets'][0]['id'] is the id of the upperlevel tweet
        # output[0]['data'] id of lower level tweet

        Tweet_replies = []
        # list to formate tweets with replies
        with open("./data/{}/{}_Main_Tweets.json".format(i, i)) as js_file:
            Main_Tweets = json.load(js_file)
        with open("./data/{}/{}_Conversation_Tweets.json".format(i, i)) as js_file:
            Conversation_Tweet = json.load(js_file)
        for inStance in Main_Tweets:
            # print(inStance)
            if 'data' in inStance:
                for data in inStance['data']:
                    # loop to iterate in main tweets
                    # print(data['possibly_sensitive'])
                    Tweet_replies.append({"author_id": data['author_id'], "conversation_id": data['conversation_id'],
                                          "created_at": data['created_at'], "id": data['id'], "lang": data['lang'],
                                          "possibly_sensitive": data['possibly_sensitive'],
                                          "public_metrics": data['public_metrics'], "text": data['text'],
                                          "replies": replies(data['id']), "source": data['source']})

        list_to_Json(Tweet_replies, "{}".format(i))
        # function which converts tweets with replies in json



