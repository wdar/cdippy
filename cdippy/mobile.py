import netCDF4

def getRealtimeStations():
    """ Get a list of realtime stations available from CDIP's thredds catalog"""
    from lxml import etree
    from urllib.request import urlopen
    parser = etree.HTMLParser()
    with urlopen('http://thredds.cdip.ucsd.edu/thredds/catalog/cdip/realtime/catalog.xml') as f:
        tree = etree.parse(f, parser)
    root = tree.getroot()
    stations = []
    for dataset in root.iterfind('.//dataset/dataset'):
        stations.append(dataset.get('name')[0:3])
    return(stations)


def b_to_string(bytes, encoding='utf-8'):
    """ Return utf-8 sting, terminating a first null byte"""
    bytes = bytes.split(b'\0',1)[0]
    string = str(bytes, encoding)
    return string
