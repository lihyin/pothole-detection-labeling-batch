import json
import os

import boto3

print('Loading function')


def lambda_handler(event, context):
    result_message = ''

    s3 = boto3.client('s3')
    output_manifest = s3.get_object(Bucket='pothole-detection-production',
                                    Key='labeled_data/pothole-detection/manifests/output/output.manifest')

    output_manifest_string = output_manifest['Body'].read()
    items_list = output_manifest_string.decode().split("\n")

    directory = "dataset"
    if not os.path.exists(directory):
        os.makedirs(directory)

    for item_string in items_list:
        item = json.loads(item_string)

        pathname = item["source-ref"] # "s3://pothole-detection-production/photo/asdasdf.jpg"
        head, tail = os.path.split(pathname)
        filename, ext = os.path.splitext(tail)

        image_width = item["pothole-detection"]["image_size"][0]["width"]
        image_height = item["pothole-detection"]["image_size"][0]["height"]

        f = open(directory + "/" + filename + ".txt", "w")
        for pothole in item["pothole-detection"]["annotations"]:
            f.write("{0} {1} {2} {3} {4}\n".format(pothole["class_id"],
                                                 (pothole["left"] + pothole["width"]/2.0) / float(image_width),
                                                 (pothole["top"] + pothole["height"]/2.0) / float(image_height),
                                                 pothole["width"] / float(image_width),
                                                 pothole["height"] / float(image_height)
                                                 ))
        f.close()

    return result_message

lambda_handler(None, None)