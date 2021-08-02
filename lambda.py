import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # msg = ""
    # Log event data received
    print("Received event: " + json.dumps(event, indent=2))
    # create S3 client
    s3 = boto3.client('s3')
    # create rekognition client
    rekog_client = boto3.client('rekognition')
    # Get bucket name
    bucket = event['Records'][0]['s3']['bucket']['name']
    # Get the object key
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    # Get event name
    event_name = event['Records'][0]['eventName']
    event_time = event['Records'][0]['eventTime']
    # Get event region
    event_region = event['Records'][0]['awsRegion']

    
    if event_name.split(':')[0] != 'ObjectRemoved':
        try:
            # msg = 'Event {0} triggered for object \"{1}\" from bucket \"{2}\" at {3} in region {4}'.format(event_name, object_key, bucket, event_time,event_region)
            response = s3.get_object(Bucket=bucket, Key=object_key)
            if response['ContentType'].split('/')[0] == 'image':

                response_labels = rekog_client.detect_labels(
                    Image={'S3Object': {'Bucket': bucket,'Name': object_key}                    
                    })
                print(response_labels)

                response_text = rekog_client.detect_text(
                    Image={'S3Object': {'Bucket': bucket,'Name': object_key}                    
                    })
                print(response_text)
            
            nosql_db = boto3.client('dynamodb')
            write_labels = nosql_db.put_item(
                TableName='ImageLables',
                Item = {
                    'object_key': object_key,
                    'Labels' : response_labels,
                    'Text' : response_text
                }
            )
        except Exception as exc:
            print('{0} Error getting object {1} from bucket {2} .'.format(exc.args[0], object_key, bucket))    
            
    return 100


