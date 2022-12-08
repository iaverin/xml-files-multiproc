from xml.etree.ElementTree import ElementTree, SubElement, Element, dump, indent
import random
import uuid
import typing
import io
import zipfile
import os

XML_MAX_OBJECTS = 10 
XML_FILES_IN_ZIP = 100
ZIP_FILES = 50 
ZIP_DIRECTORY = "zip-files"

def random_string() -> str:
    return str(uuid.uuid4())

def create_objects() -> Element:
    objects_num = random.randrange(1, XML_MAX_OBJECTS+1)
    objects = Element("objects")
    for i in range(0, objects_num):
        objects.append(Element("object", {"name": random_string()}))
    return objects    

def create_xml_tree() -> ElementTree:
    root = Element("root")
    root.append(Element("var", attrib={"name":"id", "value" : random_string()}))
    root.append(Element("var", attrib={"name":"level", "value" : str(random.randrange(1,101))}))
    root.append(create_objects())
    tree = ElementTree(root)
    indent(tree)
    return tree

def generate_xml_file_data(tree: ElementTree) -> str:
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

def create_zip_files(xml_files_in_zip, number_zip_files,  zip_files_dir):
    for zip_file_index in range(1,number_zip_files+1):
        zip_file_name = f"{zip_files_dir}/{str(zip_file_index)}.zip"
    
        try:
            with zipfile.ZipFile(zip_file_name,"w") as z:
                for i in range (1, xml_files_in_zip+1):
                    z.writestr(zipfile.ZipInfo(f"{i}.xml"), generate_xml_file_data(create_xml_tree()))
            print(f"created {zip_file_name}...")
        except OSError as e:
            print(f"Error saving zip file. \n Error: {e}")

if __name__ == "__main__":
    if not create_dir(ZIP_DIRECTORY):
        exit()
    create_zip_files(XML_FILES_IN_ZIP, ZIP_FILES, ZIP_DIRECTORY)





















        

    