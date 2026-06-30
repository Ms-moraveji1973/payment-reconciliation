import json
from app.modules.order.queue_manager import MessageQueue
from app.core.logger import log as logger

class DeadLetterQueue:
    def __init__(self, queue:MessageQueue):
        self.queue = queue
        self.redis = queue.redis
        self.dead_letter_stream = queue.dead_letter_stream

    async def get_dead_letters(self,count=10, cursor: str = '-'):
        start_id = cursor if cursor == '-' else f"({cursor}"
        dead_messages = await self.redis.xrange(self.dead_letter_stream,start_id,'+',count=count)

        parsed_message = []
        next_cursor = None

        for msg_id, fields in dead_messages:
            parsed_message.append({
                "msg_id" : msg_id,
                "data" : json.loads(fields.get('data','{}')),
                "original_id" : fields.get('original_id'),
                "attempts" : int(fields.get('attempts',0)),
                "failed_at" : float(fields.get('failed_at',0))
            })
            
        if len(dead_messages) < count:
            next_cursor = None
        else:
            next_cursor = dead_messages[-1][0]

        return {
            "messages" : parsed_message,
            "next_cursor" : next_cursor
        }


    async def replay_message(self,msg_id:str):
        message = await self.redis.xrange(self.dead_letter_stream, msg_id, msg_id)
        if not message:
            return None

        _, fields = message[0]
        fields['attempts'] = '0'
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.xadd(self.queue.stream_name, fields, id='*', maxlen=self.queue.max_len, approximate=True)
            logger.info(f"Message {msg_id} has been sent to the {self.queue.stream_name}.")
            pipe.xdel(self.dead_letter_stream, msg_id)
            logger.info(f"Message {msg_id} has been deleted from the {self.dead_letter_stream}.")
            result = await pipe.execute()
        new_msg_id = result[0]
        return {
            "success" : True,
            "new_msg_id": new_msg_id
        }
