import json
import pymysql
import os
import pandas
import boto3

print('Loading function')

mysql_config = {
    'host': os.environ['db_host'],
    'user': os.environ['db_user'],
    'password': os.environ['db_password'],
    'db': os.environ['db']
}

def lambda_handler(event, context):
    connection = pymysql.connect(host=mysql_config['host'],
                                 user=mysql_config['user'],
                                 password=mysql_config['password'],
                                 db=mysql_config['db'])
    result_message = ''
    with connection:
        sql = "SELECT uuid, longitude, latitude, device_id, image_filepath, objects, timestamp " \
            "FROM training_data WHERE is_new = 1"
        df = pandas.read_sql(sql, connection)

        output_manifest_string = ''
        for index, row in df.iterrows():
            '''
            item = {
                      "source-ref": "s3://pothole-detection/5dff5ac1-0564-4900-ae2e-ed9f244c2f07_detection_result.png",
                      "pothole-detection": {
                        "annotations": [
                          {
                            "class_id": 1,
                            "width": 17,
                            "top": 277,
                            "height": 23,
                            "left": 239
                          },
                          {
                            "class_id": 1,
                            "width": 18,
                            "top": 273,
                            "height": 17,
                            "left": 144
                          }
                        ],
                        "image_size": [
                          {
                            "width": 416,
                            "depth": 3,
                            "height": 416
                          }
                        ]
                      },
                      "pothole-detection-metadata": {
                        "job-name": "labeling-job/pothole-detection",
                        "class-map": {
                          "1": "non-pothole"
                        },
                        "human-annotated": "yes",
                        "objects": [
                          {
                            "confidence": 0.09
                          },
                          {
                            "confidence": 0.09
                          }
                        ],
                        "creation-date": "2020-03-23T16:20:26.437623",
                        "type": "groundtruth/object-detection"
                      }
                    }
            '''

            item = {"source-ref": "s3://pothole-detection-production/photo/" + row["image_filepath"],
                    "pothole-detection":
                        {"annotations": [],
                         "image_size": [
                             {
                                 "width": 416,
                                 "depth": 3,
                                 "height": 416
                             }
                         ]
                         },
                    "pothole-detection-metadata": {
                            "job-name": "labeling-job/pothole-detection",
                            "class-map": {
                                "0": "pothole",
                                "1": "non-pothole"
                            },
                            "human-annotated": "yes",
                            "objects": [],
                            "creation-date": str(row["timestamp"]),
                            "type": "groundtruth/object-detection"
                        }
                    }

            for pothole in json.loads(row["objects"]):
                item["pothole-detection"]["annotations"].append({"class_id": 0,
                                "width": pothole["boundingbox_width"],
                                "top": pothole["boundingbox_y"],
                                "height": pothole["boundingbox_height"],
                                "left": pothole["boundingbox_x"]})
                item["pothole-detection-metadata"]["objects"].append({"confidence": pothole['confidence']})

            output_manifest_string += json.dumps(item) + '\n'

            # change is_new to false
            cursor2 = connection.cursor()
            cursor2.execute("UPDATE training_data SET is_new = 0 WHERE uuid = %s", (row['uuid']))
            connection.commit()

    s3 = boto3.resource('s3')
    s3.Bucket('pothole-detection-production')\
        .put_object(Key='labeled_data/pothole-detection/manifests/output/output.manifest',
                    Body=output_manifest_string)

    print(result_message)
    connection.close()
    return result_message

lambda_handler(None, None)