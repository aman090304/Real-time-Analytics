from time import time

from fastapi import FastAPI
import redis

app = FastAPI()
r = redis.Redis(host="redis", port=6379, decode_responses=True)

@app.get("/top-routes")
def top_routes():
    return r.zrevrange("top_routes", 0, 5, withscores=True)


# @app.get("/route-traffic")
# def route_traffic(route: str):
#     now = time.time()
#     count = r.zcount(f"search:{route}", now-300, now)
#     return {"route": route, "last_5_min": count}