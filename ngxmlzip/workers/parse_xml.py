import multiprocessing
import xml.etree.ElementTree as ET
from ..data_types import DataCSVFile1, DataCSVFile2, XMLFile, ParsedXMLData
from ..queue_manager import Worker, WorkerResult


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


class ParseXMLWorker(Worker):
    def __init__(
        self,
        name: str,
        data_file_1_queue: multiprocessing.Queue,
        data_file_2_queue: multiprocessing.Queue,
    ):
        super().__init__(name)
        self.data_file_1_queue = data_file_1_queue
        self.data_file_2_queue = data_file_2_queue

    def worker(self, data: XMLFile) -> WorkerResult:
        if isinstance(data, XMLFile):
            try:
                parsed_xml_data = parse_xml_file(data.xml_data)
            except ET.ParseError as e:
                raise ValueError(
                    f"Could not parse XML in data in {data.zip_file}, {data.xml_file}. Error: {e}"
                )
            self.data_file_1_queue.put(
                DataCSVFile1(id=parsed_xml_data.id, level=parsed_xml_data.level)
            )

            self.data_file_2_queue.put(
                DataCSVFile2(
                    id=parsed_xml_data.id, object_names=parsed_xml_data.object_names
                )
            )
            return WorkerResult(records_processed=1)
        raise ValueError(
            f"Wrong data type {type(data)} format in xml data queue. Should be XMLFile"
        )
