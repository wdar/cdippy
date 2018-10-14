import cdippy.url_utils as uu
import cdippy.utils as cu

class NcHashes():
    """ Methods for working with the online list of historic nc file hashes """

    hashes_url = 'http://cdip.ucsd.edu/data_access/metadata/wavecdf_by_datemod.txt'
    new_hashes = {}
    hash_pkl = 'HASH.pkl'

    def __init__(cls):
        cls.load_hash_table()

    def load_hash_table(cls):
        lines = uu.read_url(cls.hashes_url).strip().split('\n')
        for line in lines:
            if line[0:8] == 'filename':
                continue
            fields = line.split('\t')
            cls.new_hashes[fields[0]] = fields[6]

    def get_last_deployment(cls, stn):
        last_deployment = 'd00'
        for name in cls.new_hashes:
            print(name[6:9]) 
            if name[0:5] == stn and name[5:7] == '_d' and last_deployment < name[6:9]:
                    last_deployment = name[6:9] 
        return last_deployment
         
    def compare_hash_tables(cls):
        """ Return a list of nc files that have changed or are new """
        old_hashes = cls.get_old_hashes()
        changed = []
        if old_hashes :
            if len(cls.new_hashes) == 0:
                cls.load_hash_table()
            for key in cls.new_hashes:
                if key not in old_hashes.keys() or (key in old_hashes.keys() and old_hashes[key] != cls.new_hashes[key]):
                    changed.append(key)
        return changed

    def save_new_hashes(cls):
        cu.pkl_dump(cls.new_hashes,cls.hash_pkl)

    def get_old_hashes(cls):
        return cu.pkl_load(cls.hash_pkl)


if __name__ == "__main__":

    #- Tests
    def t0():
        h = NcHashes()
        print(h.compare_hash_tables())
        h.save_new_hashes()

    t0()

