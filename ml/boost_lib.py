__author__ = 'uri lipowezkie'
import numpy as np
import scipy.io as sio
from numpy.matlib import sort


def dixon3(t_var, name):
    ts_var = sort(t_var)
    q_min = ts_var[2] - ts_var[0]
    t_int = ts_var[-3] - ts_var[0]
    qd_1 = q_min/t_int
    q_max = ts_var[-1] - ts_var[-3]
    t_int = ts_var[-1] - ts_var[2]
    qd_2 = q_max / t_int
    q_quant = 0.253
    if qd_1 > q_quant:
        print name, " has suspected minimual value = ", 
# function Qd = gDixon3(Tvar)
# % outlier q-test r22
#  Tsort = sort(Tvar);
# Qmin = Tsort(3)-Tsort(1);
# Tint = Tsort(end-2)-Tsort(1);
# Qd(1) = Qmin/Tint;
# Qmax = Tsort(end) - Tsort(end-2);
# Tint = Tsort(end) - Tsort(3);
# Qd(2) = Qmax/Tint;
# qQuant = 0.253;
# Qd = gDixon3(Fea);
# if(Qd(1)>qQuant)
#     disp([OutFname, ' minimum = ', num2str(min(Fea)), ' has suspected Dixon = ',num2str(Qd(1))]);
# end
# if(Qd(2)>qQuant)
#     disp([OutFname, ' maximum = ', num2str(max(Fea)), ' has suspected Dixon = ',num2str(Qd(2))]);