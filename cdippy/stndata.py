from datetime import datetime, timedelta
from bisect import bisect_left

import numpy.ma as ma

from cdippy.cdippy import CDIPnc, Archive, Realtime, RealtimeXY, Historic
import cdippy.timestamp_utils as tsu
import cdippy.utils as cu


class StnData(CDIPnc):
    """ 
        Returns data and metadata for the specified station. 

        This class handles the problem that neither the Realtime 
        nor the Historic .nc file may exist for either data or metadata,
        and the number of deployment files is unknown apriori.
        It tries to seam the multiple station files together.
    """

    max_deployments = 99 # Checks at most this number of deployment nc files

    # Commonly requested sets of variables
    parameter_vars = ['waveHs', 'waveTp', 'waveDp', 'waveTa']
    xyz_vars = ['xyzXDisplacement', 'xyzYDisplacement', 'xyzZDisplacement']
    spectrum_vars = [
        'waveEnergyDensity', 'waveMeanDirection', 
        'waveA1Value', 'waveB1Value', 'waveA2Value', 'waveB2Value',
        'waveCheckFactor',]
    meta_vars = [
        'metaStationName', 
        'metaDeployLatitude', 'metaDeployLongitude', 'metaWaterDepth',
        'metaDeclilnation']
    meta_attributes = [
        'wmo_id', 
        'geospatial_lat_min', 'geospatial_lat_max', 'geospatial_lat_units', 'geospatial_lat_resolution',
        'geospatial_lon_min', 'geospatial_lon_max', 'geospatial_lon_units', 'geospatial_lon_resolution',
        'geospatial_vertical_min', 'geospatial_vertical_max', 'geospatial_vertical_units', 'geospatial_vertical_resolution',
        'time_coverage_start', 'time_coverage_end',
        'date_created', 'date_modified' ]
    
    def __init__(cls, stn, data_dir=None, org=None):
        cls.nc = None
        cls.stn = stn
        cls.data_dir = data_dir
        cls.org = org
        cls.historic = Historic(cls.stn, cls.data_dir, cls.org)
        cls.realtime = Realtime(cls.stn, cls.data_dir, cls.org)
        if cls.historic and cls.historic.nc :
            cls.meta = cls.historic
        else: 
            if cls.realtime and cls.realtime.nc :
                cls.meta = cls.realtime
            else:
                return None

    def get_parameters(cls, start=None, end=None, pub_set='public', apply_mask=True, target_records=0):
        return cls.get_series(start, end, cls.parameter_vars, pub_set, apply_mask, target_records)

    def get_stn_meta(cls):
        """ Returns a dict of station meta data using historic or realtime file. """
        result = {}
        if cls.meta is None:
            return result
        cls.meta.set_request_info(vrs=cls.meta_vars)
        result = cls.meta.get_request()
        for attr_name in cls.meta_attributes:
            if hasattr(cls.meta.nc, attr_name):
                result[attr_name] = getattr(cls.meta.nc, attr_name)
        return result

    def get_xyz(cls, start=None, end=None, pub_set='public'):
        return cls.get_series(start, end, cls.xyz_vars, pub_set)

    def get_spectra(cls, start=None, end=None, pub_set='public', apply_mask=True, target_records=0):
        return cls.get_series(start, end, cls.spectrum_vars, pub_set, apply_mask, target_records)

    def get_series(cls, start=None, end=None, vrs=None, pub_set='public', apply_mask=True, target_records=0):
        """ 
            Returns a dict of data between start and end dates with specified quality. 

            Use this to get series that may span realtime and historic files. 
            If end is None, then start is considered a target date.
        """
        if vrs is None:
            vrs = cls.parameter_vars
        prefix = cls.get_var_prefix(vrs[0])

        if start is not None and end is None: # Target time
            ts_I  = cls.get_target_timespan(cu.datetime_to_timestamp(start), target_records, prefix+'Time')
            if ts_I[0] is not None:
                start = cu.timestamp_to_datetime(ts_I[0])
                end = cu.timestamp_to_datetime(ts_I[1])
            else:
                return None
        elif start is None: # Use default 3 days back
            start = datetime.utcnow()-timedelta(days=3)
            end = datetime.utcnow()

        cls.set_request_info(start, end, vrs, pub_set, apply_mask)
        if vrs is not None and prefix == 'xyz':
            return cls.merge_xyz_request()
        else:
            return cls.merge_request()

    def aggregate_dicts(cls, dict1, dict2):
        """ Aggregate the data in two dictionaries. Dict1 has oldest data. """
       #- Union the keys to make sure we check each one
        ukeys = set(dict1.keys()) | set(dict2.keys())
        result = { }
        #- Combine the variables
        for key in ukeys :
            if key in dict2 and key in dict1:
                result[key] = ma.concatenate([dict1[key], dict2[key]])
            elif key in dict2:
                result[key] = dict2[key]
            else:
                result[key] = dict1[key]
        return result

    def merge_xyz_request(cls):
        """ Merge xyz data from realtime and archive nc files. """
        if cls.vrs and cls.vrs[0] == 'xyzData':
            cls.vrs = ['xyzXDisplacement','xyzYDisplacement','xyzZDisplacement']
        request_timespan = cu.Timespan(cls.start_stamp, cls.end_stamp)
        result = {}

        def helper(cdip_nc, request_timespan, result):
            # Try the next file if it is without xyz data
            z = cdip_nc.get_var('xyzZDisplacement')
            if z is None:
                return result, cls.start_stamp
            # Try the next file if start_stamp cannot be calculated
            start_stamp = cdip_nc.get_xyz_timestamp(0)
            end_stamp = cdip_nc.get_xyz_timestamp(len(z)-1)
            if start_stamp is None:
                return result, cls.start_stamp
            file_timespan = cu.Timespan(start_stamp, end_stamp)
            # Add data if request timespan overlaps data timespan
            if request_timespan.overlap(file_timespan):
                cdip_nc.start_stamp = cls.start_stamp
                cdip_nc.end_stamp = cls.end_stamp
                cdip_nc.pub_set = cls.pub_set
                cdip_nc.apply_mask = cls.apply_mask
                cdip_nc.vrs = cls.vrs
                tmp_result = cdip_nc.get_request()
                result = cls.aggregate_dicts(result, tmp_result)
            return result, start_stamp

        # First get realtime data if it exists
        rt = RealtimeXY(cls.stn)
        if rt.nc is not None:
            result, start_stamp = helper(rt, request_timespan, result)

        # If the request start time is more recent than the realtime
        # start time, no need to look in the archives
        if cls.start_stamp > start_stamp:
            return result

        # Second, look in archive files for data
        for dep in range(1, cls.max_deployments):
            deployment = 'd'+'{:02d}'.format(dep)
            ar = Archive(cls.stn, deployment, cls.data_dir, cls.org)
            if ar.nc is None:
                break
            result, start_stamp = helper(ar, request_timespan, result)
            
            # Break if file start stamp is greater than request end stamp
            if start_stamp > cls.end_stamp :
                break
        return result

    def merge_request(cls):
        """ Returns data for given request across realtime and historic files """
        rt = {};
        r = cls.realtime
        # Note that we are assuming that waveTime will work for every time dim.
        if r.nc is not None and r.get_var('waveTime')[0] <= cls.end_stamp:
            r.vrs = cls.vrs
            r.start_stamp = cls.start_stamp
            r.end_stamp = cls.end_stamp
            r.pub_set = cls.pub_set
            r.apply_mask = cls.apply_mask
            rt = r.get_request()
        ht = {};
        h = cls.historic
        if h.nc is not None and h.get_var('waveTime')[-1] >= cls.start_stamp:
            h.vrs = cls.vrs
            h.start_stamp = cls.start_stamp
            h.end_stamp = cls.end_stamp
            h.pub_set = cls.pub_set
            h.apply_mask = cls.apply_mask
            ht = h.get_request()
        return cls.aggregate_dicts(ht, rt)

    def get_nc_files(cls, types=['realtime','historic','archive']):
        """ Returns dict of netcdf4 objects of a station's netcdf files """
        result = {}
        for type in types:
            if type == 'realtime':
                rt = Realtime(cls.stn, cls.data_dir, cls.org)
                if rt.nc:
                    result[rt.filename] = rt.nc
            if type == 'historic':
                ht = Historic(cls.stn, cls.data_dir, cls.org)
                if ht.nc:
                    result[ht.filename] = ht.nc
            if type == 'archive':
                for dep in range(1,cls.max_deployments):
                    deployment = 'd'+'{:02d}'.format(dep)
                    ar = Archive(cls.stn, deployment, cls.data_dir, cls.org)
                    if ar.nc is None:
                        break
                    result[ar.filename] = ar
        return result

    def get_target_timespan(cls, target_timestamp, n, time_var):
        """ 
            Returns a 2-tuple of timestamps, an interval corresponding to  n records to 
            the right or left of target_timestamp.
 
            Given a time_var (e.g. 'waveTime') and target timestamp, returns a 2-tuple 
            of timestamps corresponding to i and i+n (n<0 or n>=0) taken from
            the realtime and historic nc files. Those timestamps can then be used in
            set_request_info().
        """
        r_ok = False
        if cls.realtime.nc is not None:
            r_ok = True
        h_ok = False
        if cls.historic.nc is not None:
            h_ok = True

        # Check realtime to find closest index

        r_closest_idx = None
        if r_ok: 
            r_stamps = cls.realtime.get_var(time_var)[:] 
            r_last_idx = len(r_stamps) - 1
            i_b = bisect_left(r_stamps, target_timestamp)
            # i_b will be possibly one more than the last index
            i_b = min(i_b, r_last_idx)
            # Target timestamp is exactly equal to a data time 
            if i_b == r_last_idx or r_stamps[i_b] == target_timestamp:
                r_closest_idx = i_b
            elif i_b > 0:
                r_closest_idx = tsu.get_closest_index(i_b-1, i_b, r_stamps, target_timestamp)

        # If closest index not found, check historic

        h_closest_idx = None
        h_last_idx = None # Let's us know if h_stamps has been loaded
        if h_ok and not r_closest_idx:
            h_stamps = cls.historic.get_var(time_var)[:] 
            h_last_idx = len(h_stamps) - 1
            i_b = bisect_left(h_stamps, target_timestamp)
            i_b = min(i_b, h_last_idx)
            # Target timestamp is exactly equal to a data time 
            if (i_b <= h_last_idx and h_stamps[i_b] == target_timestamp) or i_b == 0:
                h_closest_idx = i_b
            elif i_b >= h_last_idx: # Target is between the two files
                if r_ok:
                    if abs(h_stamps[h_last_idx]-target_timestamp) < abs(r_stamps[0]-target_timestamp):
                        h_closest_idx = i_b
                    else:
                        r_closest_idx = 0
                else: # No realtime file 
                    h_closest_idx = i_b
            else: # Within middle of historic stamps
                h_closest_idx = tsu.get_closest_index(i_b-1, i_b, h_stamps, target_timestamp)

        # Now we have the closest index, find the intervals

        if r_closest_idx is not None:
            r_interval = tsu.get_interval(r_stamps, r_closest_idx, n)
            # If bound exceeded toward H and H exists, cacluate h_interval
            if r_interval[2] < 0 and h_ok:
                if not h_last_idx:
                    h_stamps = cls.historic.get_var(time_var)[:] 
                    h_last_idx = len(h_stamps) - 1
                h_interval = tsu.get_interval(h_stamps, h_last_idx, n+r_closest_idx+1)
                #print("Rx H interval: ", h_interval)
                #print("Rx R interval: ", r_interval)
                return tsu.combine_intervals(h_interval, r_interval)
            else:
                return r_interval 
        elif h_closest_idx is not None:
            h_interval = tsu.get_interval(h_stamps, h_closest_idx, n)
            # If bound exceeded toward R and R exists, cacluate r_interval
            if h_interval[2] > 0 and r_ok: 
                r_interval = tsu.get_interval(r_stamps, 0, n+h_closest_idx-h_last_idx-1)
                #print("Hx H interval: ", h_interval)
                #print("Hx R interval: ", r_interval)
                return tsu.combine_intervals(h_interval, r_interval)
            else:
                return h_interval 

        # If we get to here there's a problem
        return (None, None, None)

if __name__ == "__main__":
    #- Tests
    def t0():
        s = StnData('100p1')
        d = s.get_stn_meta()
        print(d)
    def t1():
        s = StnData('100p1')
        d = s.get_spectra(datetime(2016,8,1), target_records=3)
        print(d.keys())
        print(d['waveEnergyDensity'].shape)
    def t2():
        s = StnData('100p1',org='ww3')
        d = s.get_series('2016-08-01 00:00:00','2016-08-02 23:59:59',['waveHs'],'public')
        print(d)
    def t3():
        s = StnData('100p1',data_dir='./gdata')
        d = s.get_nc_files(['historic','archive','realtime'])
        print(d.keys())
    def t4():
        s = StnData('100p1')
        # Across deployments 5 and 6
        d = s.get_series('2007-05-30 00:00:00','2007-06-01 23:59:59',['xyzData'],'public')
        print(len(d['xyzXDisplacement']))
        print(len(d['xyzTime']))
        print(d['xyzTime'][0],d['xyzTime'][-1])
    def t5():
        s = StnData('100p1')
        dt = datetime(2010,4,1,0,0)
        d = s.get_series(dt, target_records=-4)
        print(d)
    def t6():
        # Mark 1 filter delay set to -999.9
        s = StnData('071p1')
        end = datetime.utcnow()
        end = datetime(1996,1,22,15,57,00)
        start = end - timedelta(hours=2)
        d = s.get_xyz(start, end)
        print("D: "+repr(d))
        print("Len: "+repr(len(d['xyzTime'])))

    t6()
