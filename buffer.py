import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, db=0)
STREAM_KEY = "data_stream"
PUBSUB_CHANNEL = "process_trigger"

BATCH_SIZE = 10

def buffer_loop():
    last_id = "0"
    buffer = []
    while True:
        resp = r.xread({STREAM_KEY: last_id}, block=5000, count=10)
        if resp:
            for stream, messages in resp:
                for msg_id, message in messages:
                    buffer.append((msg_id, message))
                    last_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                    if len(buffer) >= BATCH_SIZE:
                        # Publica notificação para processar o batch
                        notification = json.dumps({
                            "message_ids": [m[0].decode() for m in buffer if isinstance(m[0], bytes)],
                            "count": len(buffer)
                        })
                        r.publish(PUBSUB_CHANNEL, notification)
                        print(f"Buffer cheio. Notificando processamento: {notification}")
                        buffer.clear()
        else:
            # Timeout, pode fazer sleep ou checar buffer
            if buffer:
                notification = json.dumps({
                    "message_ids": [m[0].decode() for m in buffer if isinstance(m[0], bytes)],
                    "count": len(buffer)
                })
                r.publish(PUBSUB_CHANNEL, notification)
                print(f"Buffer com dados pendentes. Notificando processamento: {notification}")
                buffer.clear()
            time.sleep(1)

if __name__ == "__main__":
    buffer_loop()
