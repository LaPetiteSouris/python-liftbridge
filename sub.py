import json
from python_liftbridge import Lift, Stream
# Create a Liftbridge client.
client = Lift(ip_address='localhost:9294', timeout=5)
meta_data = client.fetch_metadata()

print(meta_data)
"""
# Subscribe to the stream starting from the beginning.
for message in client.subscribe(
        Stream(
            subject='test2',
            name='test2-stream',
            read_isr_replica=True
        ).start_at_earliest_received(), ):
    try:
        msg = json.loads(message.value)
        print("value of event is ", msg.get("event_triggered"))
    except Exception:
        pass
    """