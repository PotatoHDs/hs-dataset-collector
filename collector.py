import json
from aiohttp import ClientSession
import asyncio
import time
import aiofiles as aiof
from aiohttp.client_exceptions import ClientOSError, ServerDisconnectedError


async def fetch(game_id, session):
    url = f'https://hsreplaynet-replays.s3.amazonaws.com/viewed_replays/{game_id}.hsreplay.xml'
    res = ''
    while not res:
        try:
            async with session.get(url, timeout=10) as response:
                print(f'Fetched game {game_id}')
                res = await response.text()
        except (ClientOSError, ServerDisconnectedError, asyncio.TimeoutError) as e:
            if type(e) == asyncio.TimeoutError:
                print('Timeout error')
            else:
                print(e)
    return res


# async def write_file(data, path):
#     print(f'Writing to {path}')
#     done = False
#     while not done:
#         try:
#             async with aiof.open(path, "w") as out:
#                 await out.write(json.dumps(data))
#                 done = True
#         except (ConnectionResetError, OSError) as e:
#             print(e)


async def main():
    tasks = []
    count = 0
    # create instance of Semaphore
    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        async with session.get('https://hsreplay.net/api/v1/live/replay_feed/') as response:
            prev = set()
            while True:
                start = time.time()
                games = (await response.json())["data"]
                print('Collected games')
                if all(game['id'] not in prev for game in games):
                    for result in games:
                        task = asyncio.ensure_future(fetch(result['id'], session))
                        tasks.append(task)
                    responses = await asyncio.gather(*tasks)
                    print('Fetched all games')
                    for i in range(len(games)):
                        if responses[i]:
                            prev.add(games[i]['id'])
                            games[i]['xml'] = responses[i]
                            async with open(fr'D:\datasets and shit\hs_games\{games[i]["id"]}', "w") as out:
                                await out.write(json.dumps(games[i]))
                            # task = asyncio.ensure_future(
                            #     write_file(games[i], fr'D:\datasets and shit\hs_games\{games[i]["id"]}'))
                            # tasks.append(task)
                            count += 1
                    tasks = []
                    prev = set()

                end = time.time() - start
                if 30 - end > 0:
                    await asyncio.sleep(30 - end)
                print(count)
            # print(type(res))

        # for i in range(1, 600):
        #     # pass Semaphore and session to every GET request
        #     task = asyncio.ensure_future(fetch(f'https://pokeapi.co/api/v2/pokemon/{i}', session))
        #     tasks.append(task)
        # responses = asyncio.gather(*tasks)
        # res = await responses
        # print(len(res))


if __name__ == "__main__":
    start_time = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    # asyncio.run(main())
    print("--- %s seconds ---" % (time.time() - start_time))

# https://hsreplay.net/api/v1/live/replay_feed/
