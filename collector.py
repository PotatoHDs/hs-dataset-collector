import json
from aiohttp import ClientSession
import asyncio
import time
import aiofiles as aiof
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError


async def fetch(game_id, session, sem):
    async with sem:
        res = ''
        while not res:
            try:
                async with session.get(f'https://hsreplay.net/api/v1/games/{game_id}/', timeout=5) as response:
                    print(f'Fetched game link {game_id}')
                    link = (await response.json())['replay_xml']
                async with session.get(link, timeout=5) as response:
                    print(f'Fetched game {game_id}')
                    res = await response.text()
            except (ClientOSError, ServerDisconnectedError, asyncio.TimeoutError, KeyError) as e:
                if type(e) == asyncio.TimeoutError:
                    print('Timeout error')
                else:
                    print(e)
        return res


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
            async with session.get('https://hsreplay.net/api/v1/live/replay_feed/') as response:
                games = (await response.json())["data"]
                print('Collected games')
                # print(prev, {game['id'] for game in games}, sep='\n')
                curr = {game['id'] for game in games}
                if prev != curr:
                    curr = prev.union(curr).difference(prev)
                    for game_id in curr:
                        task = asyncio.ensure_future(fetch(game_id, session, sem))
                        tasks.append(task)
                    responses = await asyncio.gather(*tasks)
                    print('Fetched all games')
                    tasks = []
                    prev = set()
                    for i, game_id in enumerate(curr):
                        if responses[i]:
                            prev.add(game_id)
                            game = [game for game in games if game['id'] == game_id][0]
                            game['xml'] = responses[i]
                            task = asyncio.ensure_future(
                                write_file(game, fr'D:\datasets and shit\hs_games\{game_id}.json'))
                            tasks.append(task)
                            # count += 1
                    await asyncio.gather(*tasks)
                    tasks = []

            end = time.time() - start
            if 30 - end > 0:
                await asyncio.sleep(30 - end)
            # print(count)


if __name__ == "__main__":
    start_time = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    print("--- %s seconds ---" % (time.time() - start_time))

# https://hsreplay.net/api/v1/live/replay_feed/
