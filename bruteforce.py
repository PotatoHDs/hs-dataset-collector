from aiohttp import ClientSession
from string import ascii_letters, digits
from random import choice
import asyncio
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError


async def fetch(game_id, session, sem):
    async with sem:
        done = False
        link = f'https://hsreplay.net/api/v1/games/{game_id}/?format=json'
        while not done:
            try:
                # , timeout=5
                async with session.get(link) as response:
                    if 'detail' not in (await response.json()).keys():
                        print(game_id)
                    done = True
            except (ClientOSError, ServerDisconnectedError, asyncio.TimeoutError, KeyError) as e:
                if type(e) == asyncio.TimeoutError:
                    print('Timeout error')
                elif type(e) == KeyError:
                    return
                else:
                    print(e)


def generate_id(size=22, chars=ascii_letters + digits):
    return ''.join(choice(chars) for _ in range(size))

from urlparse import urlparse
from threading import Thread
import httplib, sys
from Queue import Queue

concurrent = 200

def doWork():
    while True:
        url = q.get()
        status, url = getStatus(url)
        doSomethingWithResult(status, url)
        q.task_done()

def getStatus(ourl):
    try:
        url = urlparse(ourl)
        conn = httplib.HTTPConnection(url.netloc)
        conn.request("HEAD", url.path)
        res = conn.getresponse()
        return res.status, ourl
    except:
        return "error", ourl

def doSomethingWithResult(status, url):
    print status, url

q = Queue(concurrent * 2)
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for url in open('urllist.txt'):
        q.put(url.strip())
    q.join()
except KeyboardInterrupt:
    sys.exit(1)
