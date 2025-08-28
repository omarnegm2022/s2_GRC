import os
import json
from Schema_Extraction import ss_dict




for scope in ss_dict.keys():
        os.mkdir(scope)
        for s in range(len(ss_dict[scope])):
                # abs_src = src.split('.')[0] 
                os.mkdir(f"{scope}/{ss_dict[scope][s]}")


