from typing import List, Iterable, Iterator, Generator
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


def append_csv_file_type_2(csv_file: str, data: ParsedXMLData, delimiter=","):
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        for object_name in data.object_names:
            writer.writerow([data.id, object_name])


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


def process_xml_data_worker(parsed_xml_data_queue: multiprocessing.Queue, csv_file_1, csv_file_2 ):
    i = 0

    print("Worker started..")
    while True:
        try:
            parsed_xml_data = parsed_xml_data_queue.get(block=False)
            if parsed_xml_data is None:
                break

            if parsed_xml_data == "kill":
                return True

            i += 1
            print(f"Save result {i}")
            append_csv_file_type_1(csv_file_1, parsed_xml_data)
            append_csv_file_type_2(csv_file_2, parsed_xml_data)
            
            print(f"Save result {i} finish")
            
        except Empty:
            # print("Consumer: got nothing, waiting a while...", flush=True)
            # sleep(0.5)
            continue
        # check for stop
        except KeyboardInterrupt:
            print("XML_WORKER Caught KeyboardInterrupt, finish worker")
            return False

def parse_xml_worker(xml_data_queue, parsed_data_queue):
    i = 0
    print("Worker started..")
    while True:
        try:
            xml_data = xml_data_queue.get(block=False)
            if xml_data is None:
                break

            if xml_data == "kill":
                parsed_data_queue.put("kill")
                return True

            i += 1
            print(f"Parse XML file {i}")
            parsed_xml_data = parse_xml_file(xml_data)
            parsed_data_queue.put(parsed_xml_data)
            print(f"Parse XML file {i} finish")
            
        except Empty:
            # print("Consumer: got nothing, waiting a while...", flush=True)
            # sleep(0.5)
            continue
        # check for stop
        except KeyboardInterrupt:
            print("XML_WORKER Caught KeyboardInterrupt, finish worker")
            return False


def pool_init(q2):
    global q
    q = q2


def run_multi_proc(zip_dir, csv_file_1, csv_file_2):
    create_csv_file_type_1(csv_file_1)
    create_csv_file_type_2(csv_file_2)

    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGINT, original_sigint_handler)

    xml_data_queue = multiprocessing.Manager().Queue()
    parsed_data_queue = multiprocessing.Manager().Queue()

    pool = multiprocessing.Pool(
        multiprocessing.cpu_count()
    )
    
    lock = Lock()
    
    try:
        #
        jobs = []
        job = pool.apply_async(
            parse_xml_worker, (xml_data_queue, parsed_data_queue)
        )
        
        jobs.append(job)

        job = pool.apply_async(
            process_xml_data_worker, (parsed_data_queue, csv_file_1, csv_file_2)
        )

        jobs.append(job)

        zip_files = get_zip_files(f"{zip_dir}/*.zip")
        for zip_file in zip_files:
            with zipfile.ZipFile(zip_file, mode="r") as zip:
                xml_files = get_xml_files(zip_file)
                for xml_file in xml_files:
                    print(f"Zip file {zip_file}. XML file {xml_file}")
                    xml_data = xml_from_zip(zip, xml_file)
                    xml_data_queue.put(xml_data)


        xml_data_queue.put("kill")

        for j in jobs:
            j.get()
        
        pool.close()
        pool.terminate()
        pool.join()

    except KeyboardInterrupt:
        print("Main Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
        # parsed_xml_data = parse_xml_file(xml_data)
        # # print("XML data", parsed_xml_data)
        # append_csv_file_type_1(csv_file_1, parsed_xml_data)
        # append_csv_file_type_2(csv_file_2, parsed_xml_data)



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
