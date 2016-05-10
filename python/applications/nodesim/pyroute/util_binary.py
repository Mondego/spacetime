from struct import *

def encodeLL(lat,lon):
    pLat = (lat + 90.0) / 180.0
    pLon = (lon + 180.0) / 360.0
    iLat = encodeP(pLat)
    iLon = encodeP(pLon)
    return(pack("II", iLat, iLon))

def encodeP(p):
    i = int(p * 4294967296.0)
    return(i)


def decodeLL(data):
    iLat,iLon = unpack("II", data)
    pLat = decodeP(iLat)
    pLon = decodeP(iLon)
    lat = pLat * 180.0 - 90.0
    lon = pLon * 360.0 - 180.0
    return(lat,lon)

def decodeP(i):
    p = float(i) / 4294967296.0
    return(p)
