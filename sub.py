import json
from python_liftbridge import Lift, Stream
# Create a Liftbridge client.
client = Lift(ip_address='localhost:9293', timeout=5)


# Subscribe to the stream starting from the beginning.
for message in client.subscribe(
        Stream(
            subject='test4',
            name='test4-stream',
            read_isr_replica=True
        ).start_at_earliest_received(), ):
    try:
        msg = json.loads(message.value)
        print("value of event is ", msg.get("event_triggered"))
    except Exception:
        pass
