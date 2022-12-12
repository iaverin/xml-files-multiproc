from abc import ABC, abstractmethod
import time
from typing import (
    Any,
    Dict,
    List,
    Iterable,
    Iterator,
    Generator,
    Callable,
    Optional,
    Tuple,
    Union,
)
import glob
import zipfile
import xml.etree.ElementTree as ET
import io
import os
from dataclasses import dataclass, field
import csv
import cProfile
import pstats
import multiprocessing
from queue import Empty
from time import sleep
import signal
from multiprocessing import Lock


def process(x):
    print("ok")


def get_zip_files(directory: str) -> List[str]:
    return glob.glob(directory)


def get_xml_files(zip_file) -> Generator[str, None, None]:
    with zipfile.ZipFile(zip_file, mode="r") as zip:
        files = zip.namelist()
        for f in files:
            yield f


def xml_from_zip(zipfile: zipfile.ZipFile, xml_file_name: str) -> str:
    """
    returns xml file contents from zip file as string
    throws FileNotFoundError if file not found inside zip
    """
    try:
        with zipfile.open(xml_file_name, "r") as xml_file:
            xml_data = xml_file.read().decode("utf-8")
            return xml_data
    except KeyError:
        raise FileNotFoundError(f"File not found inside zip: {xml_file_name}")


@dataclass
class ParsedXMLData:
    id: str
    level: str
    object_names: List[str]


@dataclass
class DataCSVFile1:
    id: str
    level: str


@dataclass
class DataCSVFile2:
    id: str
    object_names: List[str]


def parse_xml_file(xml_file_data: str) -> ParsedXMLData:
    root = ET.fromstring(xml_file_data)

    id = ""
    level = ""
    object_names = list()

    for child in root:
        if child.tag == "var" and child.attrib.get("name") == "id":
            id = child.attrib.get("value", "")

        if child.tag == "var" and child.attrib.get("name") == "level":
            level = child.attrib.get("value", "")

        if child.tag == "objects":
            objects = child
            for object in objects:
                object_names.append(object.attrib.get("name", ""))

    parsed_data = ParsedXMLData(id=id, level=level, object_names=object_names)
    return parsed_data


def create_csv_file_type_1(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "level"])


def create_csv_file_type_2(csv_file: str, delimiter=","):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "object_name"])


def append_csv_file_type_1(csv_file: str, data: ParsedXMLData, delimiter=","):
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow([data.id, data.level])
    return 1


def append_csv_file_type_2(csv_file: str, data: ParsedXMLData, delimiter=","):
    records_stored = 0
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        for object_name in data.object_names:
            writer.writerow([data.id, object_name])
            records_stored += 1
    return records_stored


def run(zip_dir, csv_file_1, csv_file_2):
    create_csv_file_type_1(csv_file_1)
    create_csv_file_type_2(csv_file_2)

    zip_files = get_zip_files(f"{zip_dir}/*.zip")
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, mode="r") as zip:
            xml_files = get_xml_files(zip_file)
            for xml_file in xml_files:
                print(f"Zip file {zip_file}. XML file {xml_file}")
                xml_data = xml_from_zip(zip, xml_file)
                parsed_xml_data = parse_xml_file(xml_data)
                # print("XML data", parsed_xml_data)
                append_csv_file_type_1(csv_file_1, parsed_xml_data)
                append_csv_file_type_2(csv_file_2, parsed_xml_data)


@dataclass
class QueueWorkerResult:
    errors: List[Exception]
    total_worker_calls: int
    successful_worker_calls: int
    records_processed: int
    max_queue_size: int
    queue_size_on_start: int
    context: dict = field(default_factory=dict)


@dataclass
class WorkerResult:
    records_processed: int = 0
    context: Any = field(default=None)


class Worker():
    def __init__(self, name:str = ""):
        self.name = name

    @abstractmethod
    def worker(self, data: Any, context: Any, *args) -> WorkerResult:
        pass

class ChunkedWorker():
    def __init__(self, name: str="", max_chunk_size: int=0):
        self.name = name
        self.max_chunk_size = max_chunk_size

    @abstractmethod
    def worker(self, data: List[Any], context: Any, *args) -> WorkerResult:
        pass

def queue_manager(
    queue: multiprocessing.Queue,
    worker: Union[Callable[[multiprocessing.Queue, Any], Any], Worker , ChunkedWorker],
    *args: Tuple[Any],
) -> Any:

    # print(f"Worker {worker.__name__} started..")

    result = QueueWorkerResult(
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

            if check_stop_queue(data):
                # print(f"Worker {worker.__name__} will be stopped")
                return result

            result.total_worker_calls += 1
            if isinstance(worker, Callable):
                worker_result = worker(data, *args)
                result.successful_worker_calls += 1
                if worker_result > 0:
                    result.records_processed += worker_result

            if isinstance(worker, Worker):
                worker_result = worker.worker(data, context=result.context, *args)
                result.successful_worker_calls += 1
                if worker_result.context:
                    result.context = worker_result.context
            
            if isinstance(worker, ChunkedWorker):
                chunked_data = [data,]
                end_after = False
                
                while not queue.empty() and len(chunked_data) < worker.max_chunk_size:
                    data=queue.get(block=False)
                    
                    if check_stop_queue(data):
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
            print(f"Queue {worker.__name__} Caught KeyboardInterrupt, finish worker")
            return False
        except Exception as e:
            result.errors.append(e)
            continue


def parse_xml_worker(
    data: Any,
    data_file_1_queue: multiprocessing.Queue,
    data_file_2_queue: multiprocessing.Queue,
    monitoring_queue:  multiprocessing.Queue
) -> int:

    parsed_xml_data = parse_xml_file(data)
    monitoring_queue.put({"object_parsed":len(parsed_xml_data.object_names)})
    data_file_1_queue.put(
        DataCSVFile1(id=parsed_xml_data.id, level=parsed_xml_data.level)
    )

    data_file_2_queue.put(
        DataCSVFile2(id=parsed_xml_data.id, object_names=parsed_xml_data.object_names)
    )

    return 1


def queue_file_1_worker(data, csv_file_1) -> int:
    return append_csv_file_type_1(csv_file_1, data)


def queue_file_2_worker(data, csv_file_2) -> int:
    return append_csv_file_type_2(csv_file_2, data)


class MonitoringWorker(Worker):
    def worker(self, data: Any, context: Any, *args) -> WorkerResult:
        return_context = dict(context)
        if isinstance(data, dict):
            for k,v in data.items():
                return_context[k] = context.get(k, 0) + v
        else:
            return_context[data] = context.get(data, 0) + 1
        return WorkerResult(1, context=return_context)

class CSVFile2Worker(ChunkedWorker):
    def worker(self, data: List[Any], csv_file) -> WorkerResult:
        records_stored = 0
        with open(csv_file, "a", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=",")
            for chunk in data:
                for object_name in chunk.object_names:
                    writer.writerow([chunk.id, object_name])
                    records_stored += 1
        return WorkerResult(records_processed=records_stored)


class CSVFile1Worker(ChunkedWorker):
    def worker(self, data: List[Any], csv_file) -> WorkerResult:
        records_stored = 0
        with open(csv_file, "a", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            for chunk in data:
                writer.writerow([chunk.id, chunk.level])
                records_stored += 1
        return WorkerResult(records_processed=records_stored)


def stop_queue(*args: Tuple[multiprocessing.Queue]):
    for q in args:
        q.put("kill")


def check_stop_queue(data):
    return data == "kill"


def jobs_finished(jobs: dict) -> bool:
    all_finished = True
    for name, j in jobs.items():
        all_finished = j.ready() and all_finished
    return all_finished


def print_results(
    xml_files_proceeded, csv_file_1, csv_file_1_records, csv_file_2, csv_file_2_records
):
    print(f"=======================================================================")
    print(f"XML files processed {xml_files_proceeded}")
    print(f"Records in {csv_file_1} stored {csv_file_1_records}")
    print(f"Records in {csv_file_2} stored {csv_file_2_records}")


def put_xml_from_zip_files_in_queue(
    zip_dir: str,
    xml_data_queue: multiprocessing.Queue,
    monitoring_queue: multiprocessing.Queue,
):
    zip_files = get_zip_files(f"{zip_dir}/*.zip")
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, mode="r") as zip:
            monitoring_queue.put("zip_file_extracted")
            xml_files = get_xml_files(zip_file)
            for xml_file in xml_files:
                # print(f"Zip file {zip_file}. Extracted XML file {xml_file}")
                xml_data = xml_from_zip(zip, xml_file)
                xml_data_queue.put(xml_data)
                monitoring_queue.put("xml_file_added")
                
    return


def run_multi_proc(zip_dir, csv_file_1, csv_file_2):
    create_csv_file_type_1(csv_file_1)
    create_csv_file_type_2(csv_file_2)

    # original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, original_sigint_handler)

    xml_data_queue = multiprocessing.Manager().Queue()
    data_file_1_queue = multiprocessing.Manager().Queue()
    data_file_2_queue = multiprocessing.Manager().Queue()

    monitoring_queue = multiprocessing.Manager().Queue()

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    jobs = {}
    jobs["parse_xml_worker"] = pool.apply_async(
        queue_manager,
        (xml_data_queue, parse_xml_worker, data_file_1_queue, data_file_2_queue, monitoring_queue),
    )

    jobs["parse_xml_worker_2"] = pool.apply_async(
        queue_manager,
        (xml_data_queue, parse_xml_worker, data_file_1_queue, data_file_2_queue, monitoring_queue),
    )

    jobs["parse_xml_worker_3"] = pool.apply_async(
        queue_manager,
        (xml_data_queue, parse_xml_worker, data_file_1_queue, data_file_2_queue, monitoring_queue),
    )

    jobs["save_file_1"] = pool.apply_async(
        queue_manager, (data_file_1_queue, CSVFile1Worker(max_chunk_size=1000), csv_file_1)
    )

    jobs["save_file_2"] = pool.apply_async(
        queue_manager, (data_file_2_queue, CSVFile2Worker(max_chunk_size=1000), csv_file_2)
    )

    # global monitoring
    # monitoring = MonitoringDataQueue(queue=monitoring_queue)
    monitoring_worker = MonitoringWorker()
    monitoring_job = pool.apply_async(
        queue_manager, (monitoring_queue, monitoring_worker)
    )

    # main_loop
    try:
        put_xml_from_zip_files_in_queue(zip_dir, xml_data_queue, monitoring_queue)
        stop_queue(xml_data_queue)
        stop_queue(xml_data_queue)
        stop_queue(xml_data_queue)

        while not jobs_finished(jobs):
            if jobs["parse_xml_worker"].ready():
                stop_queue(data_file_1_queue, data_file_2_queue)
        
        for name, j in jobs.items():
            print(f"{name} ready with results {j.get()} ")
        
        stop_queue(monitoring_queue)
        # monitoring_job.get()
        print(f"{monitoring_job.get()}")

    except KeyboardInterrupt:
        print("Main Caught KeyboardInterrupt, terminating workers")
        pool.terminate()

    pool.close()
    pool.join()

    print_results(
        xml_files_proceeded=jobs["parse_xml_worker"].get().records_processed,
        csv_file_1=csv_file_1,
        csv_file_1_records=jobs["save_file_1"].get().records_processed,
        csv_file_2=csv_file_2,
        csv_file_2_records=jobs["save_file_2"].get().records_processed,
    )


if __name__ == "__main__":
    # cProfile.run('run()')
    ZIP_DIRECTORY = "zip-files"
    if os.path.split(os.getcwd())[1].split(os.sep)[-1] == "ngxmlzip":
        zip_dir = f"../{ZIP_DIRECTORY}"
    else:
        zip_dir = f"{ZIP_DIRECTORY}"

    # profiler = cProfile.Profile()
    # profiler.enable()
    t_start = time.time()
    run_multi_proc(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    t_finish = time.time()
    print(f"Time spent {t_finish - t_start}")
    # run(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats("cumtime")
    # stats.print_stats()
