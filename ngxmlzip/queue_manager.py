from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import multiprocessing as mp
from multiprocessing.pool import AsyncResult
from queue import Empty
from typing import Any, Callable, Dict, List, Tuple


@dataclass
class WorkerResult:
    records_processed: int = 0
    context: Any = field(default=None)


class Worker(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def worker(self, data: Any, context: Any, *args) -> WorkerResult:
        pass


class ChunkedWorker(ABC):
    def __init__(self, name: str = "", max_chunk_size: int = 0):
        self.name = name
        self.max_chunk_size = max_chunk_size


@dataclass
class QueueWorkerResult:
    worker_name: str
    errors: list[Exception]
    total_worker_calls: int
    successful_worker_calls: int
    records_processed: int
    max_queue_size: int
    queue_size_on_start: int
    context: dict = field(default_factory=dict)


@dataclass
class QueueWorker:
    queue: mp.Queue
    worker: Worker
    instance: int
    on_finish_stop_queues: List[mp.Queue]
    job: AsyncResult = None


class QueueWorkersManager:
    def __init__(self, number_processes=1):
        self.queue_workers: List[QueueWorker] = []
        self._jobs: List[AsyncResult] = list()

    def _send_stop(self, queue) -> None:
        queue.put("kill")

    def stop_worker_instances(self, worker: Worker):
        for queue_worker in self.queue_workers:
            if queue_worker.worker.name == worker.name:
                self._send_stop(queue_worker.queue)

    @classmethod
    def _check_stop_queue(self, data) -> bool:
        return data == "kill"

    def all_workers_ready(self) -> bool:
        pass

    def add_worker(self, queue: mp.Queue, worker: Worker | ChunkedWorker):
        self.queue_workers[worker.name] = {"queue": queue, "worker": worker}

    def workers_finished(self) -> bool:
        all_finished = True
        for queue_worker in self.queue_workers:
            job = queue_worker.job
            all_finished = job.ready() and all_finished
            if job.ready():
                for q in queue_worker.on_finish_stop_queues:
                    self._send_stop(q)
        return all_finished

    def collect_results(self) -> bool:
        results = []
        for j in self._jobs:
            results.append(j.get())
        return results

    def add_job(self, job):
        self._jobs.update(job)

    def register_worker(self, queue: mp.Queue, worker: Worker, instances: int = 1, on_finish_stop_queues: List[mp.Queue] =[]):
        for i in range(0, instances):
            self.queue_workers.append(
                QueueWorker(queue=queue, worker=worker, instance=i, on_finish_stop_queues=on_finish_stop_queues)
            )

    @classmethod
    def _worker(cls, queue: mp.Queue, worker: Worker | ChunkedWorker, *args):
        result = QueueWorkerResult(
            worker_name= worker.name,
            errors=[],
            total_worker_calls=0,
            successful_worker_calls=0,
            records_processed=0,
            max_queue_size=0,
            queue_size_on_start=queue.qsize(),
        )

        while True:
            try:
                queue_size = queue.qsize()

                if queue_size > result.max_queue_size:
                    result.max_queue_size = queue_size

                data = queue.get(block=False)
                if data == None:
                    continue

                if cls._check_stop_queue(data):
                    # print(f"Worker {worker.__name__} will be stopped")
                    return result
                result.total_worker_calls += 1
                if isinstance(worker, Worker):
                    worker_result = worker.worker(data, context=result.context, *args)
                    result.successful_worker_calls += 1
                    if worker_result.context:
                        result.context = worker_result.context

                if isinstance(worker, ChunkedWorker):
                    chunked_data = [
                        data,
                    ]
                    end_after = False

                    while (
                        not queue.empty() and len(chunked_data) < worker.max_chunk_size
                    ):
                        data = queue.get(block=False)

                        if cls._check_stop_queue(data):
                            end_after = True
                            break
                        else:
                            chunked_data.append(data)

                    worker_result = worker.worker(chunked_data, *args)
                    result.successful_worker_calls += 1
                    result.records_processed += worker_result.records_processed

                    if worker_result.context:
                        result.context = worker_result.context

                    if end_after:
                        return result

            except Empty:
                continue
            except KeyboardInterrupt:
                print(
                    f"Queue {worker.__name__} Caught KeyboardInterrupt, finish worker"
                )
                return False
            except Exception as e:
                result.errors.append(e)
                continue

    def start_workers(self, pool):
        jobs = list()
        for queue_worker in self.queue_workers:
            job = pool.apply_async(
                    self._worker, (queue_worker.queue, queue_worker.worker)
                )
            jobs.append(job)
            queue_worker.job  = job
        
        self._jobs = jobs
        return self._jobs

    def stop_queues(self):
        for queues_worker in self.queue_workers:
            self._send_stop(queues_worker.queue)
