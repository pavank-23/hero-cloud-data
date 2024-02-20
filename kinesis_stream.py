import json
import boto3
import random
import csv

kdsname = 'input-stream'
region = 'us-east-1'
i = 0
clientkinesis = boto3.client('kinesis',region_name=region)

with open('EV.csv', 'r') as csvfile:
    csv_reader = csv.DictReader(csvfile, fieldnames = ["timeOfRecord", "traceVelocity1", "traceVelocity2", "motorSpeed","motorTorque", "batterySOC", "batteryCurrent"])
    data = list(csv_reader)
    data = data[1:]

while True:
	i = (i + 1) % len(data)
	id = 'id' + str(random.randint(1665586, 8888888))
	vendorId = random.randint(1, 1000000)
	d = data[i]
	for key in d:
		d[key] = float(d[key])
	d["vendorId"] = vendorId
	response = clientkinesis.put_record(StreamName = kdsname, Data = json.dumps(d), PartitionKey=id)
	print("Total ingested: " + str(i) + ", ReqID: "+ response['ResponseMetadata']['RequestId'] + ", HTTPStatusCode: "+ str(response['ResponseMetadata']['HTTPStatusCode']))