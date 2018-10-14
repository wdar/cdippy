from datetime import datetime, timedelta
import os

import netCDF4

import numpy as np
import numbers 
from bisect import bisect_left, bisect_right

import cdippy.ndbc as ndbc
import cdippy.utils as cu
import cdippy.url_utils as uu

class CDIPnc:
    """ A base class to handle CDIP nc data files located either locally or remotely. """

    domain = 'http://thredds.cdip.ucsd.edu'
    dods = 'thredds/dodsC'

    #- Load_stn_nc_files only checks for this number of deployments
    max_deployments = 99

    #- Top level data dir for nc files. Files must be within subdirectories:
    #- i.e. <data_dir>/REALTIME, <data_dir>/ARCHIVE/201p1
    data_dir = None

    # PUBLIC/NONPUB (default is public-good)
    # Nonpub data is distinguished by WFP=4 AND WFS=1, 
    # and will be found only in the Latest and Realtime files.
    # It will be in Realtime files for data prior to the public
    # frame, and after the public frame closes (if it goes offsite etc).
    # It will be in the Latest file for the above situations and
    # for any nonpub stations.
    #
    # Note that we cannot distinguish between nonpub-good and nonpub-bad data.
    #
    # The following table lists the condition required to
    # retrieve the specific subset of data, in gory detail.
    #
    # WFP=waveFlagPrimary, WFS=waveFlagSecondary.
    # 
    # Set           Condition              Alias   Note
    # -------------------------------------------------------------
    # public-good   WFP=1                  public
    # public-bad    WFP!=1 and !nonpub-all
    # public-all    !nonpub-all
    # nonpub-good   Unable to determine
    # nonpub-bad    Unable to determine
    # nonpub-all    WFP=4 and WFS=1        nonpub
    # both-goodall  WFP=1 or nonpub-all    both    public-good or nonpub-all
    # both-good     Unable to determine
    # both-bad      Unable to determine
    # both-all      No condition           all 
    pub_set_names = [
        'public-good', 'public-bad', 'public-all', 
        'nonpub-good', 'nonpub-bad', 'nonpub-all',
        'both-good',   'both-bad',   'both-all' , 'both-badall', 'both-goodall']
    pub_set_default = 'public-good' 

    # Applies the mask before data is returned
    apply_mask = True

    # REQUESTING DATA PROCEDURE
    # 1. For a given set of variables of the same type (e.g. 'wave'), 
    #   a. determine the dimension var name and if it is a time dimension
    #   b. determine the ancillary variable name (e.g. 'waveFlagPrimary'), if it exists
    # 2. If the dimension is a time dimension, find the start and end indices based on the query
    #    (Use start and end indices to subset all variables henceforth)
    # 3. Create an ancillary variable mask based on the pub set (and start, end indices if applicable)
    # 4. For each variable, 
    #    a. use start, end indices to create a masked array
    #    b. union the variables's mask with the ancillary mask
    #    c. set the new masked array variable's mask to the union mask
    # 5. Apply the mask if cls.apply_mask set True.

    def __init__(cls, data_dir=None):
        cls.nc = None
        cls.data_dir = data_dir

    def set_request_info(cls, start=datetime(1975,1,1), end=datetime.utcnow(), vrs=['waveHs'], 
        pub_set='public', apply_mask=True):
        """ 
            Initializes data request information for get_request call 

            Vrs contains var names that have the same dimension, e.g. 'waveHs','waveTp' both
            have dimension waveTime. Start and end can be strings or datetime objects.
        """
        cls.set_timespan(start, end)
        cls.pub_set = cls.get_pub_set(pub_set) # Standardize the set name
        if apply_mask is not None:
            cls.apply_mask = apply_mask
        cls.vrs = vrs

    def set_timespan(cls, start, end):
        """ Sets request timespan """
        if isinstance(start, str) :
            cls.start_dt = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
        else:
            cls.start_dt = start
        if isinstance(end, str) :
            cls.end_dt = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
        else:
            cls.end_dt = end
        cls.start_stamp = cu.datetime_to_timestamp(cls.start_dt)
        cls.end_stamp = cu.datetime_to_timestamp(cls.end_dt)

    def get_request(cls):
        """ Returns the data specified by the request info for a single nc file. """
        mask_results = {} 
        save = {}
        result = {}

        #- Check if requested variable 0 exists
        first_var = cls.get_var(cls.vrs[0])
        if first_var is None:
            return result

        # Use first var to determine the dimension, grab it and find indices
        time_dim = None
        for dim_name in first_var.dimensions:
            nc_var = cls.get_var(dim_name)
            if nc_var is None: # To handle non-existing "count" variables
                continue
            if nc_var.units[0:7] == 'seconds':
                time_dim = dim_name
                #dim_data = np.ma.asarray(cls.nc.variables[dim_name][:])
                dim_data = cls.make_masked_array(nc_var, 0, nc_var.size)
                #- find time dimension start and end indices
                s_idx, e_idx = cls.get_indices(dim_data[:], cls.start_stamp, cls.end_stamp)
                if s_idx == e_idx:
                    return result
                mask_results[time_dim] = dim_data[s_idx:e_idx]
            else: # E.g. waveFrequency (Do I want to add to result?
                save[dim_name] = cls.nc.variables[dim_name]

        # Grab the time subset of each variable 
        for v_name in cls.vrs:
            v = cls.get_var(v_name)
            if v is None:
                continue
            if len(v.dimensions) == 1 and v.dimensions[0] == 'maxStrlen64':
                arr = cls.nc.variables[v_name][:]
                result[v_name] = cls.byte_arr_to_string(arr).strip('\x00')
            elif time_dim: 
                mask_results[v_name] = cls.make_masked_array(v, s_idx, e_idx)
            else: 
                # !!! This could be a problem for 2-d arrays. Specifying end
                # index too large may reshape array?
                #
                # Also, there seems to be a bug for single values such as
                # metaWaterDepth in realtime files. Those variables have
                # no shape (shape is an empty tupble) and len(v) bombs even 
                # though v[:] returns an array with one value. 
                try:
                    v_len = len(v)
                except:
                    v_len = 1
                result[v_name] = cls.make_masked_array(v, 0, v_len)

        # Use first var to determine the ancillary variable, e.g. waveFlagPrimary
        # If there is an ancillary variable, use pub/nonpub to create a mask
        if hasattr(first_var, 'ancillary_variables'): 
            anc_names = first_var.ancillary_variables.split(' ')
            anc_name = anc_names[0]
            # Create the variable mask using pub/nonpub choice
            if not time_dim:
               s_idx = None 
            anc_mask = cls.make_pub_mask(anc_name, s_idx, e_idx) 
        else:
            anc_mask = None

        # Still a problem. 2-d vars.
        # Seems to work if the variable has no mask set. But
        # if mask set, returns 1-d var.
        for v_name in mask_results:
            if cls.apply_mask and anc_mask is not None:
                v = mask_results[v_name]
                mask_results[v_name] = v[~anc_mask]
            result[v_name] = mask_results[v_name]

        return result

    def make_masked_array(cls, nc_var, s_idx, e_idx):
        """ 
            Returns a numpy masked array of the specified start and end indices
            where e_idx is appropriate for python arrays. I.e. one more than last index
        """
        if len(nc_var.shape) <= 1:
            try:
                data = np.ma.asarray(nc_var[s_idx:e_idx])
            except:
                try:
                    data = np.ma.asarray(nc_var[s_idx:e_idx])
                except:
                    return None
            return data
        elif len(nc_var.shape) == 2:
            try:
                arr = np.ma.asarray(nc_var[s_idx:e_idx, :])
            except:
                try:
                    arr = np.ma.asarray(nc_var[s_idx:e_idx, :])
                except:
                    return None
            return arr

    def make_pub_mask(cls, anc_name, s_idx, e_idx):
        """ Make appropriate mask based on pub_set and ancillary variable. E_idx is appropriate for python arrays. """

        # No s_idx, use whole array. Otherwise time subset the anc var.
        nc_primary = cls.get_var(anc_name)
        secondary_name  = cls.get_var_prefix(anc_name)+'FlagSecondary'
        nc_secondary = cls.get_var(secondary_name)
        if s_idx is None:
            s_idx = 0
            e_idx = len(nc_primary) 
        primary_arr = nc_primary[s_idx:e_idx]
        if nc_secondary is not None:
            secondary_arr = nc_secondary[s_idx:e_idx]

        if anc_name == 'waveFlagPrimary' or anc_name == 'sstFlagPrimary':
            public_good = primary_arr==1 # Put a ~ in front for correct mask value
            if cls.pub_set == 'public-good':
                return np.ma.make_mask( ~public_good, shrink=False )

            # All of the below need the secondary array and nonpub mask
            nonpub_all = (primary_arr==4) & (secondary_arr==1) 

            # Return appropriate mask for pub designation
            if cls.pub_set == 'nonpub-all':
                return np.ma.make_mask( ~nonpub_all, shrink=False )
            elif cls.pub_set == 'public-all':
                return np.ma.make_mask( nonpub_all, shrink=False )
            elif cls.pub_set == 'both-goodall':
                return np.ma.make_mask( ~(public_good | nonpub_all), shrink=False )
            elif cls.pub_set == 'public-bad':
                return np.ma.make_mask( public_good | nonpub_all, shrink=False )
            elif cls.pub_set == 'both-badall':
                return np.ma.make_mask( public_good , shrink=False)
            else:
                return np.ma.make_mask( ~public_good, shrink=False )
        elif anc_name == 'waveFrequencyFlagPrimary':
            pass
        elif anc_name == 'gpsStatusFlags':
            return np.ma.make_mask( np.ones(len(primary_arr),dtype=bool) , shrink=False)
        else:
            return None

    def get_pub_set(cls, name):
        """ Returns standard pub/nonpub set name given a name. """
        # Aliases
        if name == 'public':
            return 'public-good'
        elif name == 'nonpub':        
            return 'nonpub-all'
        elif name == 'both':
            return 'both-goodall'
        # Translations
        elif name == 'nonpub-good':
            return 'nonpub-all'
        elif name == 'nonpub-bad':
            return 'nonpub-all'
        elif name == 'both-good':
            return 'both-goodall'
        elif name == 'both-bad':
            return 'both-badall'
        elif name in cls.pub_set_names:
            return name
        else:
            return cls.pub_set_default

    def get_var_prefix(cls, var_name):
        """ Returns 'wave' part of the string 'waveHs'. """
        s = ''
        for c in var_name:
            if c.isupper():
                break
            s += c
        return s

    def get_flag_meanings(cls, flag_name):
        """ Returns flag category values and meanings given a flag_name, e.g. 'waveFlagPrimary' """
        return cls.get_var(flag_name).flag_meanings.split(' ')

    def get_flag_values(cls, flag_name):
        """ Returns flag category values and meanings given a flag_name, e.g. 'waveFlagPrimary' """
        v = cls.get_var(flag_name)
        if flag_name[0:3] == 'gps':
            return v.flag_masks
        else:
            return v.flag_values

    def get_date_modified(cls):
        """ Returns datetime object, last time nc file was modified. """
        return datetime.strptime(cls.nc.date_modified, '%Y-%m-%dT%H:%M:%SZ')

    def get_coverage_start(cls):
        """ Returns datetime object, start time of nc file coverage. """
        return datetime.strptime(cls.nc.time_coverage_start, '%Y-%m-%dT%H:%M:%SZ')

    def get_coverage_end(cls):
        """ Returns datetime object, end time of nc file coverage. """
        return datetime.strptime(cls.nc.time_coverage_end, '%Y-%m-%dT%H:%M:%SZ')

    def get_indices(cls, times, start_stamp, end_stamp):
        """ Returns start and end indices to include any times that are equal to start_stamp or end_stamp.  """
        s_idx = bisect_left(times,start_stamp) # Will include time if equal
        e_idx = bisect_right(times,end_stamp,s_idx) # Will give e_idx appropriate for python arrays
        return s_idx, e_idx

    def get_nc(cls,url=None):
        if url is None:
            url = cls.url
        # Check if the html page or file exists
        if (url[0:4] == 'http' and not uu.url_exists(url+'.html')) and not os.path.isfile(url):
                return None
        try:
            nc = netCDF4.Dataset(url)
        except:
            # Try again if unsuccessful (nc file not ready? THREDDS problem?)
            try:
                nc = netCDF4.Dataset(url)
            except:
                nc = None
        return nc

    def byte_arr_to_string(cls, b_arr):
        if np.ma.is_masked(b_arr):
            b_arr = b_arr[~b_arr.mask]
        s = ''
        for c in b_arr[:].astype('U'):
            s += c
        return s

    def metaStationName(cls):
        """ Get list of latest stations """
        if cls.nc is None:
            return None
        return cls.byte_arr_to_string(cls.nc.variables['metaStationName'][:])

    def get_var(cls, var_name):
        """ Checks if a variable exists then returns a pointer to it """
        if cls.nc is None or var_name not in cls.nc.variables:
            return None
        return cls.nc.variables[var_name]

    def get_dataset_urls(cls):
        """ 
            Returns a dict of two lists of urls (or paths) to all CDIP station datasets.

            The top level keys are 'realtime' and 'historic'. The urls are retrieved by
            either descending into the THREDDS catalog.xml or recursively walking through data_dir sub 
            directories.

            For applications that need to use the data from multiple deployment files for
            a station, stndata:get_nc_files will load those files efficiently.
        """
        if cls.data_dir is not None:
            result = {'realtime': [], 'archive': []}
            #- Walk through data_dir sub dirs
            for (dirpath, dirnames, filenames) in os.walk(cls.data_dir):
                if dirpath.find('REALTIME') >= 0:
                    for file in filenames:
                        if os.path.splitext(file)[1] == '.nc':
                            result['realtime'].append(
                                os.path.join(dirpath, file))
                elif dirpath.find('ARCHIVE') >= 0:
                    for file in filenames:
                        if os.path.splitext(file)[1] == '.nc':
                            result['archive'].append(
                                os.path.join(dirpath, file))
            return result

        catalog_url = '/'.join([cls.domain, 'thredds', 'catalog.xml'])

        result = {}
        root = uu.load_et_root(catalog_url)
        catalogs = []
        uu.rfindta(root, catalogs, 'catalogRef', 'href')
        for catalog in catalogs:
            #- Archive data sets
            url = cls.domain + catalog
            cat = uu.load_et_root(url)
            if catalog.find('archive') >= 0:
                ar_urls = []
                uu.rfindta(cat, ar_urls, 'catalogRef', 'href')
                b_url = os.path.dirname(url)
                #- Station datasets
                ar_ds_urls = []
                for u in ar_urls:
                    url = b_url + '/' + u
                    ds = uu.load_et_root(url)
                    uu.rfindta(ds, ar_ds_urls, 'dataset', 'urlPath')
                full_urls = []
                for url in ar_ds_urls:
                    full_urls.append('/'.join([cls.domain, cls.dods, 'cdip', url[5:]]))
                result['archive'] = full_urls
            elif catalog.find('realtime') >= 0:
                rt_ds_urls = []
                uu.rfindta(cat, rt_ds_urls, 'dataset', 'urlPath')
                full_urls = []
                for url in rt_ds_urls:
                    full_urls.append('/'.join([cls.domain, cls.dods, 'cdip', url[5:]]))
                result['realtime'] = full_urls
        return result

    def set_dataset_info(cls, stn, org, dataset_name, deployment=None):
        """  
            Sets cls.stn,org,filename,url. Loads cls.nc. Arguments should all be lower case.

            Paths are:
                <top_dir>/EXTERNAL/WW3/<filename>  [filename=<stn>_<org_dir>_<dataset_name>.nc][CDIP stn like 192w3]
                <top_dir>/REALTIME/<filename> [filename=<stn>_rt.nc]
                <top_dir>/REALTIME/<filename> [filename=<stn>_xy.nc]
                <top_dir>/ARCHIVE/<stn>/<filename> [filename=<stn>_<deployment>.nc]
            Urls are:
                http://thredds.cdip.ucsd/thredds/dodsC/<org1>/<org_dir>/<filename> [org1=external|cdip,org_dir=WW3|OWI etc]
                http://thredds.cdip.ucsd/thredds/dodsC/<org1>/<dataset_name>/<filename> [dataset_name=realtime|archive]
        """
        ext = '.nc'

        if org is None:
            org = 'cdip'
        if org == 'cdip':
            org1 = 'cdip'
        else:
            org1 = 'external'
        # Org_dir follows 'external' and always uppercase (isn't used when org is cdip)
        org_dir = org.upper()
        
        # Historic and archive both use archive as a dataset_dir
        # Lowercase for url, uppercase for url
        if dataset_name == 'historic':
            dataset_dir = 'archive'
        elif dataset_name == 'realtimexy':
            dataset_dir = 'realtime'
        else:
            dataset_dir = dataset_name

        # Local paths use uppercase
        if cls.data_dir:
            org1 = org1.upper()
            dataset_dir = dataset_dir.upper()
            if org == 'cdip':
                url_pre = cls.data_dir
            else:
                url_pre = '/'.join([cls.data_dir,org1])
        else:
            url_pre = '/'.join([cls.domain, cls.dods, org1])

        # Make filename and url
        if org == 'cdip':
            if dataset_name == 'realtime':
                dataset_name = 'rt'
            elif dataset_name == 'realtimexy':
                dataset_name = 'xy'
            elif dataset_name == 'historic':
                dataset_dir = '/'.join([dataset_dir,stn])
            elif dataset_name == 'archive' and deployment:
                dataset_name = deployment
                dataset_dir = '/'.join([dataset_dir,stn])
            cls.filename = '_'.join([stn,dataset_name+ext])
            cls.url = '/'.join([url_pre,dataset_dir,cls.filename])
        else:
            if stn[3:4] == 'p' and org == 'ww3':  # Cdip stn id
                stn_tmp = ndbc.get_wmo_id(stn[0:3])
            else:
                stn_tmp = stn
            cls.filename = '_'.join([stn_tmp, org_dir, dataset_name+ext])
            cls.url = '/'.join([url_pre,org_dir,cls.filename])

        cls.stn = stn
        cls.org = org
        cls.nc = cls.get_nc()


class Latest(CDIPnc):
    """ Loads the latest_3day.nc and has methods for using that data. """


    #meta_vars = ['metaLongitude', 'metaLatitude', 'metaWaterDepth']
    #wave_params = ['waveHs', 'waveTp', 'waveDp', 'sstSeaSurfaceTemperature']

    # Do not apply the mask to get_request calls. 
    apply_mask = False

    def __init__(cls, data_dir=None):
        CDIPnc.__init__(cls, data_dir)
        cls.labels = []  # - Holds stn labels, e.g. '100p1' for this instance
        # Set latest timespan (Latest_3day goes up to 30 minutes beyond now)
        now_plus_30min = datetime.utcnow() + timedelta(minutes=30)
        now_minus_4days = datetime.utcnow() - timedelta(days=4)
        cls.set_timespan(now_minus_4days, now_plus_30min)

        # Set basic information and init cls.nc
        cls.filename = 'latest_3day.nc'
        if cls.data_dir:
            cls.url = '/'.join([cls.data_dir,'REALTIME',cls.filename])
        else:
            cls.url = '/'.join([CDIPnc.domain, CDIPnc.dods, 'cdip/realtime/latest_3day.nc'])
        cls.nc = cls.get_nc(cls.url)

    def metaStationNames(cls):
        """ Get list of latest stations """
        if cls.nc is None:
            return None
        names = []
        for name_arr in cls.nc.variables['metaStationName']:
            names.append(cls.byte_arr_to_string(name_arr))
        return names

    def metaSiteLabels(cls):
        """ Set cls.labels list withstations, e.g. ['100p1',...] """
        if cls.nc is None:
            return None
        for label_arr in cls.nc.variables['metaSiteLabel']:
            cls.labels.append(cls.byte_arr_to_string(label_arr))
        return cls.labels

    def get_latest(cls, pub_set='public',meta_vars=None, wave_params=None):
        """ Returns a 2d list of: label,stnname,time,<wave_param1>,...,<meta_var1>,... """
        if meta_vars is None:
            meta_vars = ['metaLongitude', 'metaLatitude', 'metaWaterDepth']
        meta_vars = meta_vars 
        if wave_params is None:
            wave_params = ['waveHs', 'waveTp', 'waveDp', 'sstSeaSurfaceTemperature']
        wave_params = wave_params 
        cls.pub_set = cls.get_pub_set(pub_set)
        
        # Create a mask to remove nonpub (or in general filter on pub)
        if any('wave' in s for s in wave_params):
            # Get wave variables: waveHs, waveTp, ...
            cls.vrs = wave_params
            cls.vrs.append('waveTimeOffset')
            cls.vrs.append('waveTimeBounds')
            r = cls.get_request()
            pub_mask = cls.make_pub_mask('waveFlagPrimary', None, None)
            mask = np.ma.mask_or(r['waveTimeOffset'].mask, pub_mask)
            r['waveTimeOffset'].mask = mask
            # Index to latest data for each station. If -1, then station is masked
            ixs = cls.get_latest_ixs(r['waveTimeOffset'])

        # Create a mask to remove nonpub for sst(or in general filter on pub)
        if 'sstSeaSurfaceTemperature' in wave_params:
            # Get sst variables
            cls.vrs.append('sstTime')
            cls.vrs.append('sstTimeOffset')
            cls.vrs.append('sstTimeBounds')
            r = cls.get_request()
            pub_mask = cls.make_pub_mask('sstFlagPrimary', None, None)
            mask = np.ma.mask_or(r['sstTimeOffset'].mask, pub_mask)
            r['sstTimeOffset'].mask = mask
            # Index to latest sst data for each station. If -1, then station is masked
            ixs_sst = cls.get_latest_ixs(r['sstTimeOffset'])

        # Get meta variables: lat, lon, depth
        cls.vrs = meta_vars
        m = cls.get_request()
        m['metaStationName'] = cls.metaStationNames()
        m['metaSiteLabel'] = cls.metaSiteLabels()


        # Merge wave and meta dictionaries
        z = r.copy()
        z.update(m)

        result = {}
        for pm in z:
            arr = []
            for stn in range(len(ixs)):
                if ixs[stn] >= 0:
                    if pm == 'waveTime':
                        arr.append(z['waveTime'][ixs[stn]] +
                                   z['waveTimeOffset'][ixs[stn], stn])
                    elif pm == 'waveTimeBounds':
                        arr.append(z['waveTimeBounds'][ixs[stn]] +
                                   z['waveTimeOffset'][ixs[stn], stn])
                    elif pm == 'sstTimeBounds':
                        arr.append(z['sstTimeBounds'][ixs_sst[stn]] +
                                   z['sstTimeOffset'][ixs_sst[stn], stn])
                    elif pm == 'sstTime':
                        arr.append(z['sstTime'][ixs_sst[stn]] +
                                   z['sstTimeOffset'][ixs_sst[stn], stn])
                    elif pm == 'sstSeaSurfaceTemperature':
                        arr.append(z[pm][ixs_sst[stn], stn])
                    elif pm in cls.vrs or pm == 'metaStationName' or pm == 'metaSiteLabel': 
                        arr.append(z[pm][stn])
                    else:
                        arr.append(z[pm][ixs[stn], stn])
            result[pm] = arr
        result.pop('waveTimeOffset', None)
        return result

    def has_a_number(cls, arr):
        """ Test if there is at least one number in the array """
        for x in arr:
            if isinstance(x, numbers.Number):
                return True
        return False

    def get_latest_ixs(cls, waveTimeOffset):
        s = 0
        ixs = []
        while s < waveTimeOffset.shape[1]:
            if cls.has_a_number(waveTimeOffset[:, s]):
                ixs.append(np.ma.flatnotmasked_edges(waveTimeOffset[:, s])[1])
            else:
                ixs.append(-1)
            s += 1
        return ixs

class Realtime(CDIPnc):
    """ Loads the realtime nc file for the given station. """

    def __init__(cls, stn, data_dir=None, org=None):
        CDIPnc.__init__(cls, data_dir)
        cls.set_dataset_info(stn, org, 'realtime')

class Historic(CDIPnc):
    """ Loads the historic nc file for a given station. """

    def __init__(cls, stn, data_dir=None, org=None):
        CDIPnc.__init__(cls, data_dir)
        cls.set_dataset_info(stn, org, 'historic')


class Archive(CDIPnc):
    """ Loads an archive (deployment) file for a given station and deployment. """

    def __init__(cls, stn, deployment=None, data_dir=None, org=None):
        CDIPnc.__init__(cls, data_dir)
        if not deployment:
            deployment = 'd01'
        cls.set_dataset_info(stn, org, 'archive', deployment)

    def get_idx_from_timestamp(cls, timestamp):
        t0 = cls.get_var('xyzStartTime')[0]
        r = cls.get_var('xyzSampleRate')[0]
        # Mark I will have filter delay set to fill value
        d = cls.get_var('xyzFilterDelay')
        d = 0 if d[0] is np.ma.masked else d[0]
        return int(round(r*(timestamp - t0 + d),0))

    def make_xyzTime(cls, start_idx, end_idx):
        t0 = np.ma.asarray(cls.get_var('xyzStartTime')[0])
        r = np.ma.asarray(cls.get_var('xyzSampleRate')[0])
        # Mark I will have filter delay set to fill value
        d = cls.get_var('xyzFilterDelay')
        d = 0 if d[0] is np.ma.masked else d[0]
        d = np.ma.asarray(d)
        i = np.ma.asarray(range(start_idx, end_idx))
        return t0 - d + i/r
 
    def get_xyz_timestamp(cls, xyzIndex):
        t0 = cls.get_var('xyzStartTime')[0]
        r = cls.get_var('xyzSampleRate')[0]
        # Mark I will have filter delay set to fill value
        d = cls.get_var('xyzFilterDelay')
        d = 0 if d[0] is np.ma.masked else d[0]
        if t0 and r and d >= 0:
            return t0 - d + xyzIndex/r
        else:
            return None

    def get_request(cls):
        """ Overrides the base class method to handle xyz data requests. """

        # If not an xyz request, use base class version
        if cls.get_var_prefix(cls.vrs[0]) != 'xyz':
            return super(Archive, cls).get_request()

        # xyzData is shorthand for all these vars
        if cls.vrs[0] == 'xyzData':
            cls.vrs = ['xyzXDisplacement', 'xyzYDisplacement', 'xyzZDisplacement']

        # Handle the xyz request
        start_idx = cls.get_idx_from_timestamp(cls.start_stamp)
        end_idx = cls.get_idx_from_timestamp(cls.end_stamp)
        z = cls.get_var('xyzZDisplacement')
        # Find out if the request timespan overlaps the data
        ts1 = cu.Timespan(start_idx, end_idx)
        ts2 = cu.Timespan(0, len(z)-1)
        if not ts1.overlap(ts2):
            return {}
        # Make sure the indices will work with the arrays
        start_idx = max(0,start_idx)
        end_idx = min(len(z)-1,end_idx)
        # Just calculate xyz times for the good indices
        xyzTime = cls.make_xyzTime(start_idx, end_idx)
        result = { 'xyzTime': xyzTime }
        for vname in cls.vrs:
            result[vname] = cls.get_var(vname)[start_idx:end_idx]
        return result

class RealtimeXY(Archive):
    """ Loads the realtime xy nc file for the given station. """

    def __init__(cls, stn, data_dir=None, org=None):
        CDIPnc.__init__(cls, data_dir)
        cls.set_dataset_info(stn, org, 'realtimexy')


if __name__ == "__main__":

    #- Tests
    def t0(stn,org=None,dd=None,dep=None):
        a = Archive(stn, dep, dd, org)
        a.set_request_info(start='2007-05-10 00:00:00', end='2007-05-15 23:59:59', vrs=['metaStationName'])
        d = a.get_request()
        print(d)
    def t1(stn,dd=None,org=None):
        r = Realtime(stn, dd, org)
        r.set_request_info(start=datetime(2016,8,1), end=datetime(2016,8,2), vrs=['waveEnergyDensity'], pub_set='public')
        d = r.get_request()
        print(d['waveEnergyDensity'].shape)
    def t3(stn,org=None,dd=None):
        r = Historic(stn,org,dd)
        print(r.nc)
        print(r.url)
    def t4():
        a = Archive('100p1','d05')
        a.set_request_info('2007-05-30 00:00:00','2007-06-01 23:59:59',['xyzZDisplacement'],1)
        d = a.get_request()
        print(len(d['xyzZDisplacement']))
        print(d['xyzTime'][0],d['xyzTime'][-1])
 
