from typing import Any
import unittest
import multiprocessing as mp

from ngxmlzip.queue_manager import Worker, WorkerResult, QueueWorkersManager, QueueWorkerResult 

class SimpleProducerWorker(Worker):
    def __init__(self, name: str, param: str, consumer_queue: mp.Queue):
        self.name = name
        self.param = param
        self.consumer_queue = consumer_queue

    def worker(self, data: Any, context: Any) -> WorkerResult:
        # print(f"Producer > {self.param}: {data}")
        if data == "test_param":
            self.consumer_queue.put(f"Producer > {data}")
        return WorkerResult(records_processed=1)


class SimpleConsumerWorker(Worker):
    def __init__(self, name: str):
        self.name = name

    def worker(self, data: Any, context: Any) -> WorkerResult:
        # print(f"Result > {data}")
        if data == "Producer > {data}":
            return WorkerResult(records_processed=1)


class TestQueueManager(unittest.TestCase):
    def test_create_worker(self):
        Q_SIZE = 100 
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

        for i in range(0, Q_SIZE):
            producer_queue.put(f"test_param")

        qm.stop_worker_instances(producer_worker)

        while not qm.workers_finished():
            # print(queue_manager.calls)
            continue

        results = qm.collect_results()

        expected_producer_result = QueueWorkerResult(worker_name='producer',
                   errors=[],
                   total_worker_calls=100,
                   successful_worker_calls=100,
                   records_processed=0,
                   max_queue_size=101,
                   queue_size_on_start=101,
                   context={})

        producer_total_calls = sum ([pr.total_worker_calls for pr in results if pr.worker_name == 'producer'])
        consumer_total_calls = sum ([pr.total_worker_calls for pr in results if pr.worker_name == 'consumer'])
        
        self.assertEqual(Q_SIZE, producer_total_calls)
        self.assertEqual(Q_SIZE, consumer_total_calls)


if __name__ == "__main__":
    unittest.main()
