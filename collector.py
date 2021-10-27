import json
from aiohttp import ClientSession
import asyncio
import time
import aiofiles as aiof
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError


async def fetch(game, session, sem):
    async with sem:
        res = ''
        link = f'https://hsreplay.net/api/v1/games/{game["id"]}/?format=json'
        while not res:
            try:
                async with session.get(link, timeout=5) as response:
                    print(f'Fetching game url {game["id"]}')
                    url = (await response.json())['replay_xml']
                async with session.get(url, timeout=5) as response:
                    print(f'Fetching game {game["id"]}')
                    res = await response.text()
            except (ClientOSError, ServerDisconnectedError, asyncio.TimeoutError, KeyError) as e:
                if type(e) == asyncio.TimeoutError:
                    print('Timeout error')
                elif type(e) == KeyError:
                    return
                else:
                    print(e)
        game['xml'] = res
        await write_file(game, fr'D:\datasets and shit\hs_games\{game["id"]}.json')


async def write_file(data, path):
    print(f'Writing to {path}')
    done = False
    while not done:
        try:
            async with aiof.open(path, "w") as out:
                await out.write(json.dumps(data))
                done = True
        except Exception as e:
            # (ConnectionResetError, OSError)
            print(e, type(e))


async def main():
    tasks = []
    # count = 0
    # create instance of Semaphore
    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        prev = set()
        sem = asyncio.Semaphore(200)
        while True:
            start = time.time()
            async with session.get('https://hsreplay.net/api/v1/live/replay_feed/?format=json') as response:
                games = (await response.json())["data"]
                print('Collected games')
                # print(prev, {game['id'] for game in games}, sep='\n')
                curr = {game['id'] for game in games}
                difference = prev.union(curr).difference(prev)
                print(f'Difference between prev and now is {len(difference)} games')
                if len(difference) > 200:
                    curr = difference
                    for game_id in curr:
                        game = [game for game in games if game_id == game['id']][0]
                        task = asyncio.ensure_future(fetch(game, session, sem))
                        tasks.append(task)
                    await asyncio.gather(*tasks)
                    print('Fetched all games')
                    tasks = []
                    prev = curr

            end = time.time() - start
            if 300 - end > 0:
                await asyncio.sleep(300 - end)
            print(f'It lasted for {end} seconds')
            # print(count)


if __name__ == "__main__":
    # start_time = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    # print("--- %s seconds ---" % (time.time() - start_time))

# https://hsreplay.net/api/v1/live/replay_feed/
