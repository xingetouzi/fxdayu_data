import requests
import json


class UqerScrap(object):

    COOKIES_FILE = "UqerCookies.json"

    def __init__(self, username, password, **kwargs):
        self.user = {'username': username, 'password': password}
        self.headers = {'origin': 'https://uqer.io',
                       'accept-language': 'zh-CN,zh;q=0.8',
                       'accept-encoding': 'gzip, deflate, sdch, br',
                       'accept': 'application/json, text/javascript, */*; q=0.01',
                       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) '
                                     'AppleWebKit/537.36 (KHTML, like Gecko) '
                                     'Chrome/57.0.2987.133 Safari/537.36',
                       'referer': 'https://uqer.io/labs/notebooks/get_data.nb'}
        self.cookies = self.read_cookies(kwargs.get('cookies', self.COOKIES_FILE))
        self.mercury_head = "https://gw.wmcloud.com/mercury/%s"

    @staticmethod
    def read_cookies(config):
        if isinstance(config, dict):
            return config
        else:
            try:
                c = json.load(open(config))
            except Exception as e:
                print e
                return {}

            return c

    @staticmethod
    def save_cookies(cookies, file_name=COOKIES_FILE):
        with open(file_name, 'w') as f:
            json.dump(cookies, f)

    def ensure_cookies(self):
        if self.is_logon():
            identity = self.get_identity()
            return not identity["anonymous"]

        return False

    def get_identity(self):
        return json.loads(self.request_get("https://gw.wmcloud.com/cloud/identity/uqer.json").text)

    def is_logon(self):
        return 'cloud-sso-token' in self.cookies

    def logon(self):
        if not self.ensure_cookies():
            r = requests.post("https://gw.wmcloud.com/usermaster/authenticate/v1.json", self.user)
            self.set_cookie(r.headers['set-cookie'])
            self.save_cookies(self.cookies)
            print 'Logon success: username: {username}, password{password}'.format(**self.user)
        else:
            print 'Cookies available, already logon'

    def set_cookie(self, cookie_str):
        for cookie in cookie_str.split('; '):
            cookie = cookie.split('=')
            if len(cookie) == 2:
                self.cookies[cookie[0]] = cookie[1]

    def save_data(self, file_name, path=''):
        data = self.get_data(file_name)
        with open(path+file_name, 'wb') as f:
            f.write(data)

    def get_data(self, file_name):
        url = self.mercury_head % "databooks/{}".format(file_name)
        r = self.request_get(url)
        return r.content

    def get_books(self, book='databooks'):
        url = self.mercury_head % "api/{}?recursion".format(book)
        return json.loads(self.request_get(url).text)

    def request_get(self, url, params=None, **kwargs):
        kwargs.setdefault('headers', self.headers)
        kwargs.setdefault('cookies', self.cookies)
        if self.is_logon():
            return requests.get(url, params, **kwargs)
        else:
            self.logon()
            return self.request_get(url, params, **kwargs)
