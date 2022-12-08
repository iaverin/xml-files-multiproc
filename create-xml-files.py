import io
import os
import random
import uuid
import xml.etree.ElementTree as ET
import zipfile
from typing import Callable, Any

MAX_OBJECTS_IN_XML = 10
XML_FILES_IN_ZIP = 100
ZIP_FILES = 50
ZIP_DIRECTORY = "zip-files"


def random_string() -> str:
    return str(uuid.uuid4())


def create_objects(max_objects_in_xml: int) -> ET.Element:
    objects_num = random.randrange(1, max_objects_in_xml + 1)
    objects = ET.Element("objects")
    for i in range(0, objects_num):
        objects.append(ET.Element("object", {"name": random_string()}))
    return objects


def create_xml_tree(objects_in_xml: int) -> ET.ElementTree:
    root = ET.Element("root")
    root.append(ET.Element("var", attrib={"name": "id", "value": random_string()}))
    root.append(
        ET.Element(
            "var", attrib={"name": "level", "value": str(random.randrange(1, 101))}
        )
    )
    root.append(create_objects(objects_in_xml))
    tree = ET.ElementTree(root)
    ET.indent(tree)
    return tree


def generate_xml_file_data(tree: ET.ElementTree) -> str:
    f = io.StringIO()
    tree.write(f, encoding="unicode")
    f.seek(0)
    xml_file = f.read()
    f.close()
    return xml_file


def create_dir(dir) -> bool:
    try:
        os.mkdir(dir)
        return True
    except FileExistsError:
        return True
    except OSError as e:
        print(f"Could not create directory {dir}. \n Error: {e}")
        return False


def EMPTY_FUNC(x):
    pass

 
def create_zip_files(
    objects_in_xml: int,
    xml_files_in_zip: int,
    number_zip_files: int,
    zip_files_dir: str,
    log_created_file: Callable[[str], Any] = EMPTY_FUNC,
) -> int:
    zip_files_created = 0
    for zip_file_index in range(1, number_zip_files + 1):
        zip_file_name = f"{zip_files_dir}/{str(zip_file_index)}.zip"
        with zipfile.ZipFile(zip_file_name, "w") as z:
            for i in range(1, xml_files_in_zip + 1):
                z.writestr(
                    zipfile.ZipInfo(f"{i}.xml"),
                    generate_xml_file_data(create_xml_tree(objects_in_xml)),
                )
            zip_files_created += 1
        if log_created_file != EMPTY_FUNC:
            log_created_file(f"{zip_file_name}")
    return zip_files_created


if __name__ == "__main__":
    if not create_dir(ZIP_DIRECTORY):
        exit()

    number_of_created_zip_files = 0
    try:
        number_of_created_zip_files = create_zip_files(
            MAX_OBJECTS_IN_XML,
            XML_FILES_IN_ZIP,
            ZIP_FILES,
            ZIP_DIRECTORY,
            lambda x: print(f"Created zip file {x}"),
        )
    except OSError as e:
        print(f"Error saving zip file. \n Error: {e}")

    if number_of_created_zip_files != ZIP_FILES:
        print(
            f"Not all zip files was created. Expected {ZIP_FILES}. Created {number_of_created_zip_files}"
        )
    else:
        print(f"All of {ZIP_FILES} zip files created in {ZIP_DIRECTORY}")
