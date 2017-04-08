# encoding:utf-8


LOCAL = ('', 8080)


class Streamer(object):

    def start(self):
        pass

    def stop(self):
        pass

    def on_request(self, request):
        pass


class StreamerHandler(object):

    def set_streamer(self, code, client):
        pass

    def get_streamer(self, code):
        pass

    def config(self):
        pass

