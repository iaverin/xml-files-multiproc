from xml.etree.ElementTree import ElementTree, SubElement, Element, dump, indent
import random
import uuid
import typing
import io
import zipfile

XML_MAX_OBJECTS = 10 
XML_FILES_IN_ZIP = 100
ZIP_FILES = 50 

def create_xml_elements() -> ElementTree:
    def random_string():
        return str(uuid.uuid4())

    root = Element("root")
    root.append(Element("var", attrib={"name":"id", "value" : random_string()}))
    root.append(Element("var", attrib={"name":"level", "value" : str(random.randrange(1,101))}))

    def create_objects():
        objects_num = random.randrange(1, XML_MAX_OBJECTS+1)
        objects = Element("objects")
        for i in range(0, objects_num):
            objects.append(Element("object", {"name": random_string()}))
        return objects    

    root.append(create_objects())
    return root

def generale_xml_file_data(elements: Element):
    f = io.BytesIO()
    tree = ElementTree(element=elements)
    indent(tree)
    try:
        tree.write(f)
        f.seek(0)
        xml_file = f.read() 
        f.close()
        return xml_file 
    
    except Exception as e:
        print(f"Error creating xml file data {e}")


ZIP_FOLDER = "zip-files"

for zip_file_index in range(1,ZIP_FILES+1):
    zip_file_name = f"{ZIP_FOLDER}/{str(zip_file_index)}.zip"
    
    try:
        with zipfile.ZipFile(zip_file_name,"w") as z:
            for i in range (1, XML_FILES_IN_ZIP+1):
                z.writestr(zipfile.ZipInfo(f"{i}.xml"), generale_xml_file_data(create_xml_elements()))
        print(f"created {zip_file_name}...")
        
    except Exception as e:
        print("Error saving zip file... ", e)
























        

    