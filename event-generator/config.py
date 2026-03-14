import random

EVENT_TYPES = ["search", "click", "booking"]

AIRPORTS = [
    "DEL","LHR","JFK","DXB","SIN","CDG","FRA","AMS"
]

DEVICES = ["mobile","desktop","tablet"]

def random_airport():
    return random.choice(AIRPORTS)