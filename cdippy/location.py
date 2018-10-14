from math import radians, sin, cos, sqrt, asin, atan2, degrees

class Location:

    """ A class to work with latitude/longitude locations """

    R = 6372.8 # Earth radius in kilometers
    kmToNm = .539957 #1KM ==  .539957 NM

    def __init__(cls, latitude, longitude):
        cls.latitude = latitude
        cls.longitude = longitude

    def write_lat(cls):
        return repr(cls.latitude)+" N"

    def write_lon(cls):
        return repr(cls.longitude)

    def write_loc(cls):
        return cls.write_lat()+" "+cls.write_lon()

    def decimalMin_Loc(cls):
        longitude = cls.longitude
        if (longitude < 0):
            longitude  = longitude  * -1
        dmLon = divmod(longitude, 1)
        minLon = dmLon[1]
        minLon = minLon * 60
        minLon = format(minLon,'2.3f')
        latitude = cls.latitude
        if (latitude < 0):
            latitude  = latitude  * -1
        dmLat = divmod(latitude, 1)
        minLat = dmLat[1]
        minLat = minLat * 60
        minLat = format(minLat,'2.3f')

        if (cls.longitude < 0):
            dLon  = dmLon[0] * -1
            dLon = int(dLon)
        else:
            dLon  = dmLon[0]
            dLon = int(dLon)

        if (cls.latitude < 0):
            dLat  = dmLat[0] * -1
            dLat = int(dLat)
        else:
            dLat  = dmLat[0]
            dLat = int(dLat)

        return {'dlon':dLon,'mlon':minLon,'dlat':dLat,'mlat':minLat}


    def get_distance(cls, loc):
        """
        Return the distance in nautical miles after calculating the distance between
        two different geographical location
        """
        rdn_longitude, rdn_latitude, loc.longitude, loc.latitude = map(radians, [cls.longitude, cls.latitude, loc.longitude, loc.latitude ])

        dLat = loc.latitude - rdn_latitude
        dLon = loc.longitude - rdn_longitude

        a = sin(dLat/2)**2 + cos(rdn_latitude)*cos(loc.latitude )*sin(dLon/2)**2
        c = 2*asin(sqrt(a))
        return  (cls.R * c * cls.kmToNm)

    def get_distance_formatted(cls, loc):
        """
        Return the distance in nautical miles formatted to two decimal places, after calculating the distance between
        two different geographical location
        """
        return (format(cls.get_distance(loc), '.2f'))

    def get_direction(cls, loc):
        """
        The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
        returns angle in degrees
        """
        rdn_longitude, rdn_latitude, loc.longitude, loc.latitude = map(radians, [cls.longitude, cls.latitude, loc.longitude, loc.latitude ])

        dLon = loc.longitude  - rdn_longitude
        x = sin(dLon)*cos(loc.latitude)
        y = cos(rdn_latitude)*sin(loc.latitude) - sin(rdn_latitude)*cos(loc.latitude)*cos(dLon)
        direction = atan2(x, y)
        direction = degrees (direction)
        direction = (direction + 360 ) % 360
        return direction

if __name__ == "__main__":

    #- Tests
    def t0(lat, lon):
        #print(repr(lat))
        l = Location(lat,lon)
        print(l.write_loc())

    def t1(lat, lon):
        l = Location(lat,lon)
        print(l.write_lat())

    def t2(lat1, lon1,lat2,lon2):
        l1 = Location(lat1,lon1)
        l2 = Location(lat2,lon2)
        print(repr(l1.get_distance(l2)))
        #print(repr(l2.get_direction(l1)))

    def t3(lat1, lon1,lat2,lon2):
        l1 = Location(lat1, lon1)
        result = l1.decimalMin_Loc()
        print (l1.write_loc())
        print("Lon: " + str(result['dlon']) + " " + str(result['mlon']))
        print("Lat: " + str(result['dlat']) + " " + str(result['mlat']))

    def t4(lat1, lon1,lat2,lon2):
        l1 = Location(lat1,lon1)
        l2 = Location(lat2,lon2)
        print(repr(l1.get_distance_formatted(l2)))

    #t2(21.6689, -158.1156, 21.66915, -158.11487)
    #t3(21.6689, -158.1156, 21.66915, -158.11487)
    #t4(21.6689, -158.1156, 21.66915, -158.11487)
