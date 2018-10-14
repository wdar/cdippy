import pandas as pd
from datetime import datetime
from cdippy.stndata import StnData

class NcStats(StnData):
    """ 
        For a given station, has methods to produce data availability information. 
        
        There are methods to return counts for the entire station record to be
        used diretly by a web app, and there are methods to save to disk availabililty
        counts (e.g. xyz counts) for individual nc files. In that case updates
        to totals would be calculated by re-summarizing any files that have changed
        and adding up all the files to produce new totals.
    """ 

    flags = ['waveFlagPrimary','sstFlagPrimary','gpsStatusFlags']

    def __init__(cls,stn, data_dir=None):

        StnData.__init__(cls, stn, data_dir)

        cls.date_modifieds = {}
        cls.start = datetime.strptime('1975-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        cls.end = datetime.utcnow()
        cls.pub_set = 'both-all'

    def make_stats(cls):
        result = {}
        result['flag_counts'] = cls.flag_counts()
        result['deployments'] = cls.deployment_summary()
        return result

    def deployment_summary(cls):
        cls.load_nc_files()
        result = {}
        dep_cnt = 0
        for nc_name in cls.nc_files:
            dep = nc_name[-6:-3]
            if dep[0:1] == 'd':
                dep_cnt += 1
                result[dep] = {}
                result[dep]['time_coverage_start'] = cls.nc_files[nc_name].get_coverage_start()
                result[dep]['time_coverage_end'] = cls.nc_files[nc_name].get_coverage_end()
        result['number_of_deployments'] = dep_cnt
        return result

    def load_nc_files(cls,types=['realtime','historic','archive']):
        """ Returns dict of netcdf4 objects of a station's netcdf files """
        cls.nc_files = cls.get_nc_files(types)

    def load_file(cls,nc_filename):
        """ Sets cls.nc for a given nc_filename """
        if nc_filename in cls.nc_files:
            cls.nc = cls.nc_files[nc_filename]
        else:
            cls.nc = cls.get_nc(cls.filename_to_url(nc_filename))

    def load_date_modifieds(cls):
        pass
    def store_date_modified(cls):
        pass
        
    def nc_file_summaries(cls):
        cls.load_nc_files()
        result = {}
        for nc_name in cls.nc_files:
            result[nc_name] = cls.nc_file_summary()
        return result

    def nc_file_summary(cls, nc_filename):
        """ Summarizes a single nc file given its basename. """ 
        if cls.nc is None:
            cls.load_file(nc_name)
        result = {}
        #- Currently have just one summary
        result['flag_counts'] = cls.flag_counts()
        return result

    def flag_counts(cls, flags=None):
        """ Returns pandas dataframe of counts of flag variables for the entire station record. """
        result = {'totals':{},'by_month':{}}
        if not flags:
            flags = cls.flags
        for flag_name in flags:
            dim = cls.meta.get_var_prefix(flag_name)
            cls.data = cls.get_series(cls.start, cls.end, [flag_name], cls.pub_set)
            cat_var = cls.make_categorical_flag_var(flag_name)
            result['totals'][flag_name] = cls.total_count(cat_var)
            result['by_month'][flag_name] = cls.by_month_count(cat_var, dim)
        return result
        
    def total_count(cls, cat_var):
        """ Returns pandas dataframe of count totals for a given flag variable. """
        return pd.DataFrame({'cnt':cat_var}).groupby(cat_var).count()

    def by_month_count(cls, cat_var, dim):
        """ Returns pandas dataframe of Counts by month for a given flag variable. """
        df = pd.DataFrame({'cnt':cat_var},index=pd.to_datetime(cls.data[dim+'Time'],unit='s'))
        mon_map = df.index.map(lambda x: str(x.year)+str('{:02d}'.format(x.month)))
        return df.groupby([mon_map,cat_var]).count().fillna(0).astype(int)

    def make_categorical_flag_var(cls, flag_name):
        cat = pd.Categorical(cls.data[flag_name], categories=cls.meta.get_flag_values(flag_name))
        return cat.rename_categories(cls.meta.get_flag_meanings(flag_name))


if __name__ == "__main__":
    #- Tests
    def t1():
        av = NcStats('100p1')
        print(av.make_stats())

    #t1()
