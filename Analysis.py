# -*- coding: utf-8 -*-
"""Analysis_Optimised (1).ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1N6jz7L88KklqjH3ZRQzhXbM-8Df12BNp
"""

'''pip
install
setuptools

pip
install
germansentiment

pip
install
utils

pip
install
transformers'''
from germansentiment import SentimentModel
import json
import pandas as pd
from glob import glob

def twitterDataset(path):
    """
    This function is to divide data into the main tweets and reply tweets.
    main : a list of main tweets
    replies: a list of reply tweets

    Key list
    Main keys:
    ['author_id', 'conversation_id', 'created_at', 'id', 'lang', 'possibly_sensitive', 'public_metrics',
    'text', 'replies', 'source']
    We use 'lang', 'text', and 'replies'.

    Reply keys:
    ['author_id', 'conversation_id', 'created_at', 'id', 'lang', 'possibly_sensitive', 'public_metrics',
    'in_reply_to_status_id', 'referenced_tweets', 'reply_settings', 'source', 'text', 'replies']
    In case that there is no any reply, 'replies' key is not generated.
    """
    n = 0
    with open(path) as json_data:
        data = json.load(json_data)
        Main_Tweets = []
        #         Replies=[]
        for i in data:
            Main_Tweets.append(i['text'])
            replies = i['replies']
            for j in replies:
                reply_levels(j, n)

    return Main_Tweets, Replies


Replies = []


def reply_levels(j, n):
    if len(Replies) <= n:
        Replies.append([])
    Replies[n].append(j['text'])

    if j['public_metrics']['reply_count'] != 0:
        rep = j['replies']
        n = n + 1
        # print(n)
        # replies'{}'.format(n)=j['replies']
        for r in rep:
            j = r
            reply_levels(j, n)

    return


def category_to_label(data):
    """
    This function is to convert categorical string labels into numerical labels.
    """
    labels = []
    count_neutral = 0
    count_neg = 0
    count_pos = 0
    for d in data:
        if d == 'neutral':
            labels.append(1)
            count_neutral += 1
        elif d == 'negative':
            labels.append(0)
            count_neg += 1
        elif d == 'positive':
            labels.append(2)
            count_pos += 1
        else:
            print("Something wrong: ", d)

    print(f"Positive: {count_pos}, Neutral: {count_neutral}, Negative: {count_neg} tweets")
    return labels, count_pos, count_neutral, count_neg



def average(lst):
    return sum(lst) / len(lst)


if __name__ == "__main__":


    get_datafile = glob('data/*/*.json')
    user_data = []
    for data in get_datafile:
        if data.split('_')[-1] != 'Tweets.json':
            user_data.append(data)
    print(user_data)
    # Load Bert Model

    model = SentimentModel()

    for filepath in user_data:
        print(filepath)
        # try:
        user = filepath.split('/')[-1].split('.')[0]
        print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        print("User:", user)
        Replies = []

        # Load Data
        """
        This function is to rearrange main and reply tweets (on the first level) from the meta dataset.
        """
        main, replies = twitterDataset(filepath)

        # Data Processing
        """
        You should build a function for text clearning.
        """
        # ------>

        # Inference


        # batch_size = 3000
        # n = int(len(replies)/batch_size)
        # if len(replies) < batch_size:
        #     reply_pred = model.predict_sentiment(replies)

        main_pred = model.predict_sentiment(main)
        print("Prediction on the main tweets")
        main_pred, count_pos_main, count_neutral_main, count_neg_main = category_to_label(main_pred)
        print(f"Main User Tendency: {average(main_pred)}")
        res = [average(main_pred), count_pos_main, count_neutral_main, count_neg_main]
        index = ['AvgMain', 'PosMain', 'NeuMain', 'NegMain']

        for a in range(len(replies)):
            batch_size = 3000
            n = int(len(replies[a]) / batch_size)
            if len(replies[a]) < batch_size:
                reply_pred = model.predict_sentiment(replies[a])
            else:
                try:
                    reply_pred = []
                    for i in range(n):
                        if i < n - 1:
                            reply_pred += model.predict_sentiment(replies[a][i * batch_size:(i + 1) * batch_size])
                        else:
                            reply_pred += model.predict_sentiment(replies[a][i * batch_size:])
                except:
                    break
                # reply_pred=model.predict_sentiment(replies[a])
            reply_pred, count_pos_reply, count_neutral_reply, count_neg_reply = category_to_label(reply_pred)
            res.extend([average(reply_pred), count_pos_reply, count_neutral_reply, count_neg_reply])
            index.extend(['AvgRep{}'.format(a + 1), 'PosRep{}'.format(a + 1), 'NeuRep{}'.format(a + 1),
                          'NegRep{}'.format(a + 1)])
            print('level{}'.format(a + 1))
        """
        AvgMain: Main User Tendency
        PosMain: Number of positive main tweets
        NeuMain: Number of neutral main tweets
        NegMain: Number of negative main tweets
        AvgRep: Reply Tendency
        PosRep: Number of positive replies
        NeuRep: Number of neutral replies
        NegRep: Number of negative replies
         """
        # df = pd.DataFrame(res, index=['AvgMain','PosMain','NeuMain','NegMain','AvgRep','PosRep','NeuRep','NegRep'], columns=[user])

        df = pd.DataFrame(res)
        df = pd.DataFrame(res, index, columns=["Analysis",user])
        df.to_csv(user + '.csv')  # , index=False

    # except:
    #     print("NA")
