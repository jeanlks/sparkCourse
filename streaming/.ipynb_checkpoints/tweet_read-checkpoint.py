
# coding: utf-8
import tweepy
from tweepy import OAuthHandler

from tweepy import Stream

from tweepy.streaming import StreamListener
import socket
import json

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

class TweetListener(StreamListener):
    
    def __init__(self, csocket):
        self.client_socket = csocket
        
    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("ERROR",e)
            return True
        
    def error(self, status):
        print(status)
        return True


def send_data(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetListener(c_socket))
    twitter_stream.filter(track=['bolsonaro'])
        

if __name__ == '__main__':
    s = socket.socket()
    host = '127.0.0.1'
    port = 5555
    s.bind((host, port))
    print('Listening on port {}'.format(port))

    s.listen()
    c,addr = s.accept()

    send_data(c)
    
   

