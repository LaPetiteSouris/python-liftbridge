import json
from python_liftbridge import Lift, Message, Stream, ErrStreamExists

# Create a Liftbridge client.
client = Lift(ip_address='localhost:9292', timeout=5)

# Subscribe to the stream starting from the beginning.
for message in client.subscribe(
        Stream(
            subject='foo',
            name='foo-stream',
        ).start_at_latest_received(), ):
    try:
        msg = json.loads(message.value)
        print("value of event is ", msg.get("event_triggered"))
    except Exception as e:
        print("empty")