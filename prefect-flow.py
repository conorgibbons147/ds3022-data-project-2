# prefect flow goes here
import time
import requests
import boto3
from prefect import flow, task, get_run_logger
import json


url = "https://j9y2xa0vx0.execute-api.us-east-1.amazonaws.com/api/scatter/hjd3db"

# helper function to get sqs client in each task
def get_sqs():
    return boto3.client("sqs")

# task to do initial scatter of the messages into the queue
@task
def scatter():
    logger = get_run_logger()
    payload = requests.post(url).json()
    qurl = payload["sqs_url"]
    logger.info(f"Queue populated at: {qurl}")
    return qurl

# task to monitor the queue until all messages are available
@task
def monitor_queue(qurl: str):
    logger = get_run_logger()
    sqs = get_sqs()
    expected = 21 # expect 21
    start = time.time()
    max_wait = 900 
    interval = 5 # check every 5 seconds

    while True:
        # get queue attribute counts
        attributes = sqs.get_queue_attributes(
            QueueUrl=qurl,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
            ],
        )["Attributes"]

        visible = int(attributes["ApproximateNumberOfMessages"])
        not_visible = int(attributes["ApproximateNumberOfMessagesNotVisible"])
        delayed = int(attributes["ApproximateNumberOfMessagesDelayed"])
        total = visible + not_visible + delayed

        # log current status
        logger.info(
            f"Queue status: visible={visible}, not_visible={not_visible}, "
            f"delayed={delayed}, total={total}"
        )

        # exit when all messages exist and none are delayed
        if total >= expected and delayed == 0:
            logger.info("All messages ready for pickup.")
            break

        # end if max wait time is exceeded
        if time.time() - start > max_wait:
            logger.warning("Max wait reached; proceeding anyway.")
            break

        time.sleep(interval)

# once messages are available, this task fetches them and stores them in memory
@task
def fetch_and_store(qurl: str):
    logger = get_run_logger()
    sqs = get_sqs()
    expected = 21
    data = {}

    while len(data) < expected:
        response = sqs.receive_message(
            QueueUrl=qurl,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
        )

        # if the response doesn't contain 'Messages', skip and try again
        if "Messages" not in response:
            logger.info("No messages this poll; trying again")
            continue

        # loop through each message
        for msg in response["Messages"]:
            try:
                order_no_str = msg["MessageAttributes"]["order_no"]["StringValue"]
                word = msg["MessageAttributes"]["word"]["StringValue"]
                order_no = int(order_no_str)
                data[order_no] = word
                logger.info(f"Collected fragment {order_no}: {word} ({len(data)}/{expected})")

                # delete message after processing
                receipt = msg["ReceiptHandle"]
                sqs.delete_message(QueueUrl=qurl, ReceiptHandle=receipt)

            except KeyError as e:
                logger.warning(f"Missing key {e} in message, skipping.")
                continue
            except Exception as e:
                logger.warning(f"Unexpected error parsing message: {e}")
                continue

    # return words in correct order
    ordered_words = [data[i] for i in sorted(data)]
    logger.info("All messages received and parsed.")
    return ordered_words

# task to arrange the sorted words into a phrase
@task
def arrange_phrase(words: list[str]):
    logger = get_run_logger()
    phrase = " ".join(words)
    logger.info(f"Arranged phrase: {phrase}")
    return phrase

# final task to send the solution back via sqs
@task
def send_solution(uvaid: str, phrase: str, platform: str):
    logger = get_run_logger()
    sqs = get_sqs()
    url = "https://sqs.us-east-1.amazonaws.com/440848399208/dp2-submit"
    message = "answer"

    try:
        response = sqs.send_message(
            QueueUrl=url,
            MessageBody=message,
            MessageAttributes={
                'uvaid': {
                    'DataType': 'String',
                    'StringValue': uvaid
                },
                'phrase': {
                    'DataType': 'String',
                    'StringValue': phrase
                },
                'platform': {
                    'DataType': 'String',
                    'StringValue': platform
                }
            }
        )
        logger.info(f"Response: {response}")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            logger.info("Message successfully submitted")
        else:
            logger.warning(f"Unexpected status code: {status}")

    except Exception as e:
        logger.error(f"Failed to send message: {e}")

# final flow to orchestrate tasks, outputs used in subsequent tasks
@flow
def main_flow():
    logger = get_run_logger()
    uvaid = "hjd3db"
    qurl = scatter()                    
    monitor_queue(qurl)                   
    words = fetch_and_store(qurl)          
    phrase = arrange_phrase(words)        
    send_solution(uvaid, phrase, "prefect")
    logger.info("Flow completed successfully")
    return phrase

if __name__ == "__main__":
    main_flow()