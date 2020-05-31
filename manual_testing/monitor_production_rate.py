import datetime as dt
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from confluent_kafka import Consumer
import argparse


def append_point_to_plot(i, times, production_rate):
    offset_change = get_offset_change()

    if offset_change is not None:
        times.append(dt.datetime.now().strftime('%H:%M:%S.%f'))
        production_rate.append(offset_change)

        # Truncate x and y lists to 20 items
        # times = times[-20:]
        # production_rate = production_rate[-20:]

        ax.clear()
        ax.plot(times, production_rate)
        plt.xticks(rotation=45, ha='right')
        plt.subplots_adjust(bottom=0.30)
        plt.ylabel('Messages per interval')


def create_consumer(broker: str) -> Consumer:
    conf = {'bootstrap.servers': broker, 'group.id': 'offset_monitor', 'session.timeout.ms': 6000,
            'auto.offset.reset': 'latest', 'max.in.flight.requests.per.connection': 1, 'queued.min.messages': 1,
            'enable.auto.offset.store': False, 'enable.auto.commit': False}
    return Consumer(conf)


def get_offset_change():
    global current_high_offset
    consumer.subscribe([args.topic])
    msg = consumer.poll(timeout=1.0)
    if msg is None or msg.error():
        return None
    else:
        new_offset = msg.offset()
        offset_change = new_offset - current_high_offset
        current_high_offset = new_offset
        return offset_change


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost:9092")
    parser.add_argument("--topic", help="Monitor high offset of this topic", default="forwarder_output")
    parser.add_argument("--update-period", help="Update plot every X seconds", default=10)
    args = parser.parse_args()

    # Create figure for plotting
    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)
    x_values = []
    y_values = []

    # Create consumer
    consumer = create_consumer(args.broker)

    current_high_offset = 0

    update_period_ms = args.update_period * 1000
    ani = animation.FuncAnimation(fig, append_point_to_plot, fargs=(x_values, y_values), interval=update_period_ms)
    plt.show()
