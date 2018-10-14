""" Methods for working with urllib scraping web pages """

import xml.etree.ElementTree as ET
import urllib.request

def rfindta(el, r, tag, attr):
    """ Recursively find tags with value tag and attribute attr and append to list r """
    if len(el) > 0:
        for c in el:
            rfindta(c, r, tag, attr)
    if el.tag.find(tag) >= 0:
        for child in el.attrib:
            if child.find(attr) >= 0:
                r.append(el.attrib[child])

def rfindt(el, r, tag):
    """ Recursively find tags with value tag append to list r """
    if len(el) > 0:
        for c in el:
            rfindt(c, r, tag)
    if el.tag.find(tag) >= 0:
        print(el.text)
        r.append(el.text)

def url_exists(url):
    req = urllib.request.Request(url)
    try:
        urllib.request.urlopen(req)
    except:
        return False
    else:
        return True

def read_url(url):
    try:
        r = urllib.request.urlopen(url).read().decode('UTF-8')
    except:
        return None
    return r

def load_et_root(url):
    return ET.fromstring(urllib.request.urlopen(url).read())
