"""Utility functions for parsing XML data into Python objects."""

import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any


def xml2py(node: ET.Element) -> Any:
    """Recursively convert an XML element into a dynamic Python object.

    Each XML element becomes an instance of a dynamically created class whose
    name matches the element tag. Attributes are set as object attributes.
    Child elements are grouped into lists under the attribute named by the
    child tag. Text content is stored as ``text`` and ``value``; if a ``type``
    attribute is present the value is cast to ``float``, ``int``, or
    ``datetime`` accordingly.

    Parameters
    ----------
    node : xml.etree.ElementTree.Element
        XML element to convert.

    Returns
    -------
    Any
        Dynamically typed Python object representing *node* and its subtree.
    """
    name = node.tag

    pytype = type(name, (object,), {})
    pyobj = pytype()

    for attr in node.attrib.keys():
        setattr(pyobj, attr, node.get(attr))

    if node.text and node.text.strip() != "" and node.text.strip() != "\n":
        setattr(pyobj, "text", node.text)
        setattr(pyobj, "value", node.text)
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


def xml2obj(file_name: str) -> Any:
    """Parse an XML file or URL into a dynamic Python object hierarchy.

    Parameters
    ----------
    file_name : str
        Path to a local XML file or an HTTP/HTTPS URL starting with
        ``"http"``.

    Returns
    -------
    Any
        Root Python object produced by ``xml2py`` representing the full XML
        document.
    """
    if file_name[0:4] == "http":
        with urllib.request.urlopen(file_name) as f:
            tree = ET.parse(f)
            xml_root = tree.getroot()
    else:
        xml_root = ET.parse(file_name).getroot()
    obj = xml2py(xml_root)

    return obj
