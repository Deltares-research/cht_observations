from datetime import datetime

import urllib
import xml.etree.ElementTree as ET


def xml2py(node):
    name = node.tag

    pytype = type(name, (object,), {})
    pyobj = pytype()

    for attr in node.attrib.keys():
        setattr(pyobj, attr, node.get(attr))

    if node.text and node.text.strip() != "" and node.text.strip() != "\n":
        setattr(pyobj, "text", node.text)
        setattr(pyobj, "value", node.text)
        # Convert
        if node.attrib:
            if "type" in node.attrib.keys():
                if node.attrib["type"] == "float":
                    lst = node.text.split(",")
                    if len(lst) == 1:
                        pyobj.value = float(node.text)
                    else:
                        float_list = [float(s) for s in lst]
                        pyobj.value = float_list
                elif node.attrib["type"] == "int":
                    if "," in node.text:
                        pyobj.value = [int(s) for s in node.text.split(",")]
                    else:
                        pyobj.value = int(node.text)
                elif node.attrib["type"] == "datetime":
                    pyobj.value = datetime.strptime(node.text, "%Y%m%d %H%M%S")

    for cn in node:
        if not hasattr(pyobj, cn.tag):
            setattr(pyobj, cn.tag, [])
        getattr(pyobj, cn.tag).append(xml2py(cn))

    return pyobj


def xml2obj(file_name):
    """Converts an xml file to a python object."""

    if file_name[0:4] == "http":
        with urllib.request.urlopen(file_name) as f:
            tree = ET.parse(f)
            xml_root = tree.getroot()
    else:
        xml_root = ET.parse(file_name).getroot()
    obj = xml2py(xml_root)

    return obj
