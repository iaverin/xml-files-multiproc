from typing import List, Iterable, Iterator, Generator
import glob
from create import ZIP_DIRECTORY
import zipfile
import xml.etree.ElementTree as ET
import io
import os
from dataclasses import dataclass
import csv


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


def parse_xml_file(xml_file_data) -> ParsedXMLData:
    root = ET.fromstring(xml_file_data)
    print(root.tag)

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


def create_csv_file_type_1(csv_file: str, delimiter=','):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "level"])


def create_csv_file_type_2(csv_file: str, delimiter=','):
    with open(csv_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow(["id", "object_name"])


def append_csv_file_type_1(csv_file: str, data: ParsedXMLData, delimiter =","):
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        writer.writerow([data.id, data.level])

def append_csv_file_type_2(csv_file: str, data: ParsedXMLData, delimiter =","):
    with open(csv_file, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=delimiter)
        for object_name in data.object_names:
            writer.writerow([data.id, object_name])


if os.path.split(os.getcwd())[1].split(os.sep)[-1] == "ngxmlzip":
    zip_dir = f"../{ZIP_DIRECTORY}"
else:
    zip_dir = f"{ZIP_DIRECTORY}"

zip_files = get_zip_files(f"{zip_dir}/*.zip")
zip_file = zip_files[0]
xml_file = next(get_xml_files(zip_file))

with zipfile.ZipFile(zip_file, mode="r") as zip:
    xml_data = xml_from_zip(zip, xml_file)
    parsed_xml_data = parse_xml_file(xml_data)
    print("XML data", parsed_xml_data)

create_csv_file_type_1("csv_file_1.csv")
create_csv_file_type_2("csv_file_2.csv")

append_csv_file_type_1("csv_file_1.csv", parsed_xml_data)
append_csv_file_type_2("csv_file_2.csv", parsed_xml_data)