import cv2
import numpy as np
from pyzbar.pyzbar import decode
from pprint import pprint
import boto3


def get_table(table_name):
    # Instantiate your dynamo client object
    client = boto3.client('dynamodb')
    # Get an array of table names associated with the current account and endpoint.
    response = client.list_tables()

    if table_name in response['TableNames']:
        table_found = True
    else:
        table_found = False
        # Get the service resource.
        dynamodb = boto3.resource('dynamodb')

        # Create the DynamoDB table called table_name
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=
            [
                {
                    'AttributeName': 'barcodeData',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=
            [
                {
                    'AttributeName': 'barcodeType',
                    'AttributeType': 'N'
                }
            ],
            ProvisionedThroughput=
            {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )


def put_data(barcodeData, barcodeType, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    get_table('Barcode_data')
    table = dynamodb.Table('Barcode_data')
    response = table.put_item(
        Item={
            'barcode': barcodeData,
            'barcodeType': barcodeType
        }
    )
    return response


def decoder(image):
    gray_img = cv2.cvtColor(image, 0)
    barcode = decode(gray_img)

    for obj in barcode:
        points = obj.polygon
        (x, y, w, h) = obj.rect
        pts = np.array(points, np.int32)
        pts = pts.reshape((-1, 1, 2))
        cv2.polylines(image, [pts], True, (0, 255, 0), 3)

        barcodeData = obj.data.decode("utf-8")
        barcodeType = obj.type
        string = "Data " + str(barcodeData) + " | Type " + str(barcodeType)

        cv2.putText(frame, string, (x, y), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (255, 0, 0), 2)
        data_resp = put_data(barcodeData, barcodeType)
        print("Put data succeeded:")
        print("Barcode: " + barcodeData + " | Type: " + barcodeType)
        pprint(data_resp, sort_dicts=False)


cap = cv2.VideoCapture(0)
while True:
    ret, frame = cap.read()
    decoder(frame)
    cv2.imshow('Image', frame)
    code = cv2.waitKey(10)
    if code == ord('q'):
        break
