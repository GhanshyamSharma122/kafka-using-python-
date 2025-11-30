from flask import Flask,request,jsonify
import os
from partition_log import PartitionLog
app=Flask(__name__)
LOG_DIR='./broker_data'
os.makedirs(LOG_DIR,exist_ok=True)
topics={}
def get_log(topic:str):
    if topic not in topics:
        topic_dir=os.path.join(LOG_DIR,topic)
        topics[topic]=PartitionLog(topic_dir,segment_max_bytes=1024*1024)
    return topics[topic]
@app.route("/produce",methods=['POST'])
def produce():
    data=request.get_json()
    topic=data["topic"]
    payload=data["payload"].encode('utf-8')
    log=get_log(topic)
    offset=log.append(payload)
    return jsonify({"offset":offset})
@app.route("/fetch",methods=["GET"])
def fetch():
    topic=request.args.get("topic")
    offset=int(request.args.get('offset'))
    max_bytes=int(request.args.get('max_bytes',4096))
    log=get_log(topic)
    records=log.read(offset,max_bytes=max_bytes)
    result=[
            {'offset':off,'payload':payload.decode('utf-8')}
            for off,payload in records
            ]
    return jsonify({'records':result})
if __name__=='__main__':
    print('broker running on http://localhost:5000')
    app.run(host='0.0.0.0',port=5000)

