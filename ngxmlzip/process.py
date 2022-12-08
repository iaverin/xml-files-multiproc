from typing import List, Iterable, Iterator, Generator
import glob
from create import ZIP_DIRECTORY
import zipfile
import xml.etree.ElementTree as ET
import io
import os


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


def parse_xml_file(xml_file_data):
    pass


if os.path.split(os.getcwd())[1].split(os.sep)[-1] == "ngxmlzip":
    zip_dir = f"../{ZIP_DIRECTORY}"
else:
    zip_dir = f"{ZIP_DIRECTORY}"

zip_files = get_zip_files(f"{zip_dir}/*.zip")
zip_file = zip_files[0]
xml_file = next(get_xml_files(zip_file))

with zipfile.ZipFile(zip_file, mode="r") as zip:
    print(xml_from_zip(zip, xml_file))
