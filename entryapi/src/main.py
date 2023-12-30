from fastapi import FastAPI
from pydantic import BaseModel
from confluent_kafka import Producer
#from icecream import ic
import json
import uuid

app = FastAPI()

class KafkaItem(BaseModel):
    name: str

kafkaTopic = 'testTopic'
kafkaAddress = 'redpanda-0:9092'
kafkaConsumerGroup = 'mygroup'
p = Producer({'bootstrap.servers': kafkaAddress})

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.post("/produce")
async def create_item(item: KafkaItem):
    exportJson = json.dumps(item.model_dump(mode='json'))
    random_uuid = uuid.uuid4()
    p.produce(kafkaTopic, exportJson, str(random_uuid))
    return item
