""" 
    Utilities for working with arrays of timestamps 
    Generally, i = index, T = array of ordered timestamps, t = timestamp
"""

def get_closest_index(i1, i2, T, t0):
    """ Returns the index i such that abs(T[i]-t0) is minimum for i1,i2 """
    if abs(T[i1]-t0) <= abs(T[i2]-t0):
        return i1
    else:
        return i2
    
def get_interval(T, i, n):
    """ 
        Returns a 3-tuple (T[i], T[i+n], bounds_exceeded) where -inf < n < inf 
        and bounds_exceeded is -1 or +1 if exceeded left or right respectively.
    """
    bounds_exceeded = 0
    last_T_idx = len(T)-1

    if i+n > last_T_idx:
        bounds_exceeded = 1
    elif  i+n < 0:
        bounds_exceeded = -1

    if n >= 0:
        return (T[i], T[min(i+n,last_T_idx)], bounds_exceeded)
    else:
        return (T[max(0,i+n)], T[i], bounds_exceeded)

def combine_intervals(I1, I2):
    """ Return a 2-tuple of timestamps (I1[0], I2[1]). """
    return (I1[0], I2[1])

