from time import sleep
from typing import Any
import multiprocessing as mp
from pprint import pprint

from queue_manager import Worker, WorkerResult, QueueWorkersManager


class SimpleProducerWorker(Worker):
    def __init__(self, name: str, param: str, consumer_queue: mp.Queue):
        self.name = name
        self.param = param
        self.consumer_queue = consumer_queue

    def worker(self, data: Any, context: Any) -> WorkerResult:
        print(f"Producer > {self.param}: {data}")
        self.consumer_queue.put(f"{self.param} {data}")
        return WorkerResult(records_processed=1)


class SimpleConsumerWorker(Worker):
    def __init__(self, name: str):
        self.name = name

    def worker(self, data: Any, context: Any) -> WorkerResult:
        print(f"Result > {data}")
        return WorkerResult(records_processed=1)


def run():

    producer_queue = mp.Manager().Queue()
    consumer_queue = mp.Manager().Queue()

    producer_worker = SimpleProducerWorker(
        name="producer", param="test_param", consumer_queue=consumer_queue
    )
    consumer_worker = SimpleConsumerWorker("consumer")

    pool = mp.Pool(mp.cpu_count())

    qm = QueueWorkersManager(mp.cpu_count())

    qm.register_worker(
        producer_queue,
        producer_worker,
        instances=1,
        on_finish_stop_queues=[consumer_queue]
    )
    qm.register_worker(consumer_queue, consumer_worker, instances=2)

    jobs = qm.start_workers(pool)

    for i in range(0, 100):
        producer_queue.put(f"{i} asdfdasda")

    qm.stop_worker_instances(producer_worker)

    while not qm.workers_finished():
        # print(queue_manager.calls)
        continue

    pprint(qm.collect_results())


if __name__ == "__main__":
    run()
