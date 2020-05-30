import json
from random import randint
from time import sleep
import uuid
from python_liftbridge import Lift, Message, Stream, ErrStreamExists


def my_random_string(string_length=10):
    """Returns a random string of length string_length."""
    random = str(uuid.uuid4())
    random = random.upper()
    random = random.replace("-", "")
    return random[0:string_length]


# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Create a stream attached to the NATS subject "foo".
try:
    client.create_stream(Stream(subject='test', name='test-stream'))
except ErrStreamExists:
    print('This stream already exists!')

# Publish a message to "foo".
while True:
    msg = {"event_triggered": my_random_string(5)}
    client.publish(Message(value=json.dumps(msg), stream='foo-stream'))
    random_us = randint(10, 100) / 1000000.0
    sleep(random_us)