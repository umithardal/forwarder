from confluent_kafka import TopicPartition
from confluent_kafka import Consumer
import uuid
from helpers.f142_logdata import LogData
from pytictoc import TicToc


class MsgErrorException(Exception):
    pass


def get_all_available_messages(consumer):
    """
    Consumes all available messages topics subscribed to by the consumer
    :param consumer: The consumer object
    :return: list of messages, empty if none available
    """
    messages = []
    low_offset, high_offset = consumer.get_watermark_offsets(
        consumer.assignment()[0], cached=False
    )
    number_of_messages_available = high_offset - low_offset
    while len(messages) < number_of_messages_available:
        message = consumer.poll(timeout=2.0)
        if message is None or message.error():
            continue
        messages.append(message)
    return messages


def get_last_available_status_message(cons, status_topic):
    """

    :param cons:
    :param status_topic:
    :return: The last status message.
    """
    partitions = cons.assignment()
    _, hi = cons.get_watermark_offsets(partitions[0], cached=False, timeout=2.0)
    last_msg_offset = hi - 1
    cons.assign([TopicPartition(status_topic, partition=0, offset=last_msg_offset)])
    status_msg, _ = poll_for_valid_message(cons, expected_file_identifier=None)
    return status_msg


def poll_for_valid_message(consumer, expected_file_identifier=b"f142", timeout=15.0):
    """
    Polls the subscribed topics by the consumer and checks the buffer is not empty or malformed.
    Skips connection status messages.

    :param consumer: The consumer object
    :param expected_file_identifier: The schema id we expect to find in the message
    :param timeout: give up if we haven't found a message with expected_file_identifier after this length of time
    :return: Tuple of the message payload and the key
    """
    timer = TicToc()
    timer.tic()
    while timer.tocvalue() < timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise MsgErrorException(
                "Consumer error when polling: {}".format(msg.error())
            )

        if expected_file_identifier is None:
            return msg.value(), msg.key()
        elif expected_file_identifier is not None:
            message_file_id = msg.value()[4:8]
            assert (
                message_file_id == expected_file_identifier
                or message_file_id == b"ep00"
            ), f"Expected message to have schema id of {expected_file_identifier}, but it has {message_file_id}"
            if message_file_id == b"f142":
                return LogData.LogData.GetRootAsLogData(msg.value(), 0), msg.key()


def create_consumer(offset_reset="earliest"):
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "default.topic.config": {"auto.offset.reset": offset_reset},
        "group.id": uuid.uuid4(),
    }
    cons = Consumer(**consumer_config)
    return cons