from typing import Any, Dict, List, Iterable, Iterator, Generator, Callable, Tuple
import glob
import zipfile
import xml.etree.ElementTree as ET
import io
import os
from dataclasses import dataclass
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
    errors: list[Exception]
    successful_calls: int
    records_processed: int


def queue_manager(
    queue: multiprocessing.Queue,
    worker: Callable[[multiprocessing.Queue, Any], Any],
    *args: Tuple[Any],
) -> Any:

    print(f"Worker {worker.__name__} started..")

    result = QueueWorkerResult(errors=[], successful_calls=0, records_processed=0)

    while True:
        try:
            data = queue.get(block=False)
            if data == None:
                continue

            if check_stop_queue(data):
                print(f"Worker {worker.__name__} will be stopped")
                return result

            records_processed = worker(data, *args)
            if records_processed > 0:
                result.successful_calls += 1
                result.records_processed += records_processed

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
) -> int:

    parsed_xml_data = parse_xml_file(data)

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
    zip_dir: str, xml_data_queue: multiprocessing.Queue
):
    zip_files = get_zip_files(f"{zip_dir}/*.zip")
    for zip_file in zip_files:
        with zipfile.ZipFile(zip_file, mode="r") as zip:
            xml_files = get_xml_files(zip_file)
            for xml_file in xml_files:
                # print(f"Zip file {zip_file}. Extracted XML file {xml_file}")
                xml_data = xml_from_zip(zip, xml_file)
                xml_data_queue.put(xml_data)


def run_multi_proc(zip_dir, csv_file_1, csv_file_2):
    create_csv_file_type_1(csv_file_1)
    create_csv_file_type_2(csv_file_2)

    # original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    # signal.signal(signal.SIGINT, original_sigint_handler)

    xml_data_queue = multiprocessing.Manager().Queue()
    data_file_1_queue = multiprocessing.Manager().Queue()
    data_file_2_queue = multiprocessing.Manager().Queue()

    pool = multiprocessing.Pool(multiprocessing.cpu_count())

    jobs = {}
    jobs["parse_xml_worker"] = pool.apply_async(
        queue_manager,
        (xml_data_queue, parse_xml_worker, data_file_1_queue, data_file_2_queue),
    )

    jobs["save_file_1"] = pool.apply_async(
        queue_manager, (data_file_1_queue, queue_file_1_worker, csv_file_1)
    )

    jobs["save_file_2"] = pool.apply_async(
        queue_manager, (data_file_2_queue, queue_file_2_worker, csv_file_2)
    )

    # main_loop
    try:
        put_xml_from_zip_files_in_queue(zip_dir, xml_data_queue)
        stop_queue(xml_data_queue)
        while not jobs_finished(jobs):
            if jobs["parse_xml_worker"].ready():
                stop_queue(data_file_1_queue, data_file_2_queue)
    except KeyboardInterrupt:
        print("Main Caught KeyboardInterrupt, terminating workers")
        pool.terminate()

    pool.close()
    pool.join()

    for name, j in jobs.items():
        print(f"{name} ready with results {j.get()} ")

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

    run_multi_proc(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    # run(zip_dir, "csv_file_1.csv", "csv_file_2.csv")
    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats("cumtime")
    # stats.print_stats()
