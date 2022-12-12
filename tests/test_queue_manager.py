from typing import Any
import unittest
import multiprocessing as mp

from ngxmlzip.queue_manager import Worker, WorkerResult, QueueWorkersManager 

class SimpleWorker(Worker):
    def __init__(self, name: str, param: str):
        self.name = name
        self.param = param 

    def worker(self, data: Any, context: Any) -> WorkerResult:
        print(f" {self.param}: {data}")
        return WorkerResult(records_processed=1)

class TestQueueManager(unittest.TestCase):
    def test_create_worker(self):

        worker = SimpleWorker(name ="test", param="test_param")
        queue = mp.Manager().Queue()
        pool = mp.Pool(mp.cpu_count())

        queue_manager = QueueWorkersManager(mp.cpu_count())
        # job = queue_manager.start_worker(queue, worker)
        job = pool.apply_async(queue_manager._worker, (queue, worker))
        # queue_manager.start_worker(queue=queue, worker=worker)

        queue.put("asdfdasda")
        queue.put("asdfdasda")
        queue.put("asdfdasda")
        queue.put("asdfdasda")
        queue.put("asdfdasda")
        queue.put("kill")
        
        while not job.ready():
            continue

        print(job.get())   

        self.assertEqual("foo", "foo")


if __name__ == "__main__":
    unittest.main()
