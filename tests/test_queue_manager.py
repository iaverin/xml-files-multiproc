from typing import Any, List
import unittest
import multiprocessing as mp

from ngxmlzip.queue_manager import (
    ChunkedWorker,
    QueueAllWorkerInstancesResult,
    Worker,
    WorkerResult,
    QueueWorkersManager,
    QueueWorkerResult,
)


class SimpleProducerWorker(Worker):
    def __init__(self, name: str, param: str, consumer_queue: mp.Queue):
        self.name = name
        self.param = param
        self.consumer_queue = consumer_queue

    def worker(self, data: Any) -> WorkerResult:
        # print(f"Producer > {self.param}: {data}")
        if data == "test_param":
            self.consumer_queue.put(f"Producer > {data}")
        return WorkerResult(records_processed=1)


class SimpleConsumerWorker(Worker):
    def worker(self, data: Any) -> WorkerResult:
        # print(f"Result > {data}")
        if data == "Producer > test_param":
            return WorkerResult(records_processed=1)


class ChunkedConsumerWorker(ChunkedWorker):
    def worker(self, data_chunk: List[Any]) -> WorkerResult:
        records_processed = 0
        for data in data_chunk:
            if data == "Producer > test_param":
                records_processed += 1
        return WorkerResult(records_processed=records_processed)


class ConsumerWorkerWithError(Worker):
    def worker(self, data: Any) -> WorkerResult:
        if data % 2 == 0:
            raise ValueError(f"Error in worker {data}")
        return WorkerResult(1)


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
            on_finish_stop_queues=[consumer_queue],
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

        producer_total_calls = sum(
            [pr.total_worker_calls for pr in results if pr.worker_name == "producer"]
        )
        consumer_total_calls = sum(
            [pr.total_worker_calls for pr in results if pr.worker_name == "consumer"]
        )

        pool.close()
        pool.join()

        self.assertEqual(Q_SIZE, producer_total_calls)
        self.assertEqual(Q_SIZE, consumer_total_calls)

    def test_chunked_worker(self):
        Q_SIZE = 100
        producer_queue = mp.Manager().Queue()
        consumer_queue = mp.Manager().Queue()

        producer_worker = SimpleProducerWorker(
            name="producer", param="test_param", consumer_queue=consumer_queue
        )
        consumer_worker = ChunkedConsumerWorker("consumer", max_chunk_size=10)

        pool = mp.Pool(mp.cpu_count())

        qm = QueueWorkersManager(mp.cpu_count())

        qm.register_worker(
            producer_queue,
            producer_worker,
            instances=1,
            on_finish_stop_queues=[consumer_queue],
        )
        qm.register_worker(consumer_queue, consumer_worker, instances=2)

        jobs = qm.start_workers(pool)

        for i in range(0, Q_SIZE):
            producer_queue.put(f"test_param")

        qm.stop_worker_instances(producer_worker)

        while not qm.workers_finished():
            continue

        results = qm.collect_results()

        producer_results: QueueAllWorkerInstancesResult = qm.worker_results(
            results, producer_worker
        )
        consumer_results: QueueAllWorkerInstancesResult = qm.worker_results(
            results, consumer_worker
        )

        pool.close()
        pool.join()

        self.assertEqual(Q_SIZE, producer_results.total_worker_calls)
        self.assertEqual(Q_SIZE, producer_results.records_processed)
        self.assertEqual(1, producer_results.max_chunk_size)

        self.assertGreaterEqual(Q_SIZE, consumer_results.total_worker_calls)
        self.assertEqual(Q_SIZE, consumer_results.records_processed)
        self.assertGreaterEqual(
            consumer_worker.max_chunk_size, consumer_results.max_chunk_size
        )

    def test_errors_collection(self):

        Q_SIZE = 100
        queue = mp.Manager().Queue()
        worker = ConsumerWorkerWithError(name="worker")
        pool = mp.Pool(mp.cpu_count())
        qm = QueueWorkersManager(mp.cpu_count())
        qm.register_worker(queue, worker, instances=2)
        qm.start_workers(pool)
        for i in range(0, Q_SIZE):
            queue.put(i)
        qm.stop_worker_instances_for_queue(queue)
        while not qm.workers_finished():
            continue
        results = qm.collect_results()
        pool.close()
        pool.join()

        worker_results = qm.worker_results(results, worker)
        self.assertEqual(50, len(worker_results.errors))


if __name__ == "__main__":
    unittest.main()
