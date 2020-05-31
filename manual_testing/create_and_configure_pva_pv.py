import logging
import cothread
from p4p.nt import NTScalar
from p4p.server import Server
from p4p.server.cothread import SharedPV
import argparse
from forwarder.kafka.kafka_helpers import create_producer
from typing import List, Dict

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.ERROR)

types = {
    False: NTScalar("I"),
    True: NTScalar("d"),
}


class LazyCounter(object):
    def __init__(self, interval_s):
        self.timer = None
        self.count = 0
        self.pv = None
        self.select = False
        self.active = False
        self.interval = interval_s

    def onFirstConnect(self, pv):
        _log.info("First client connects")
        if self.timer is None:
            # start timer if necessary
            self.timer = cothread.Timer(self.interval, self._tick, retrigger=True)
        self.pv = pv
        self.active = True

    def _tick(self):
        if not self.active:
            _log.info("Close")
            # no clients connected
            if self.pv.isOpen():
                self.pv.close()
                # self.select = not self.select  # toggle type for next clients

            # cancel timer until a new first client arrives
            self.timer.cancel()
            self.pv = self.timer = None

        else:
            NT = types[self.select]

            if not self.pv.isOpen():
                _log.info("Open %s", self.count)
                self.pv.open(NT.wrap(self.count))

            else:
                _log.info("Tick %s", self.count)
                self.pv.post(NT.wrap(self.count))
            self.count += 1

    def onLastDisconnect(self, pv):
        _log.info("Last client disconnects")
        # mark in-active, but don't immediately close()
        self.active = False

    def put(self, pv, op):
        # force counter value
        self.count = op.value().value
        op.done()


def configure_forwarder(pv_names: List[str], broker: str, topic: str, config_topic: str):
    producer = create_producer("localhost:9092")
    for pv_name in pv_names:
        config_message = (
            '{'
            '  "cmd": "add",'
            '  "streams": ['
            '    {'
            f'      "channel": "{pv_name}",'
            '      "channel_provider_type": "pva",'
            '      "converter": {'
            '        "schema": "f142",'
            f'        "topic": "{broker}/{topic}"'
            '      }'
            '    }'
            '  ]'
            '}'
        )
        producer.produce(config_topic, config_message.encode("utf8"))
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--suffix", help="PV names will be pva:counter_<SUFFIX>_<0-19>", default=1)
    parser.add_argument("--broker", help="Address for the Kafka broker", default="localhost:9092")
    parser.add_argument("--topic", help="Monitor high offset of this topic", default="forwarder_output")
    parser.add_argument("--config-topic", help="Config topic Forwarder is listening to", default="forwarder_config")
    args = parser.parse_args()

    # Start up 10 PVs each 100Hz update freq
    pvs: Dict[str, SharedPV] = {}
    for pv_num in range(20):
        pv_name = f"pva:counter_{args.suffix}_{pv_num}"
        pvs[pv_name] = SharedPV(handler=LazyCounter(0.01))

    with Server(providers=[pvs]):
        try:
            configure_forwarder(list(pvs.keys()), args.broker, args.topic, args.config_topic)
            cothread.WaitForQuit()
        except KeyboardInterrupt:
            pass
