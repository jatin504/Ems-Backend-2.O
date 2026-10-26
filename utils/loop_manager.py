import asyncio
from threading import Thread

main_loop = asyncio.get_event_loop()
asyncio.set_event_loop(main_loop)

def get_main_loop():
    return main_loop

def start_loop():
    Thread(target=main_loop.run_forever, daemon=True).start()
