# import pandas as pd
import os
import json as js

scopes = [1,2,3]
path_to_csv = ''
path_to_csv += "PL_1/"
# for folder in (os.listdir(path_to_csv)):
#     if folder.split()[0] == "Scope":
#             path_to_csv += folder+'/'
#             for src in os.listdir(path_to_csv):
#                   print(os.listdir(path_to_csv+src))
#     path_to_csv = "PL_1/"

ff_dict = dict()

with open("PL_1/Schema_Manifest.json",'r') as js_file:#Lazy to call close, so I made this way =D
        ss_dict = js.load(js_file)
def read_FFs(src_dict):
        global ff_dict, path_to_csv
        for scope in src_dict.keys():
                for sources in range(len(src_dict[scope])):
                                                                        # for src_name in map(lambda x: x.split('.')[0],src_dict[scope][sources].keys()):
                                path_to_csv = f'{path_to_csv}/{scope}/'         #{src_name}/'
                                files = os.listdir(path_to_csv)
                                # print(files); exit(0)
                                files.sort(key=lambda x: int(x.split('.')[0]))
                        #NOTE: bypassing the Lexicographical Sorting (for i)

                                ff_dict[f'{path_to_csv}'] = {path_to_csv+x:dict() for x in files}
                                # list(
                                #         map(lambda x: path_to_csv+x, files)
                                #         )
                                path_to_csv = "PL_1/"
                # break
read_FFs(ss_dict)

schema_start = ['"id"','"Scope"']
lvl_labels = ['"Level 1"','"Level 2"', '"Level 3"', '"Level 4"', '"Level 5"']
schema_end = ['"UOM"','"GHG/Unit"','"GHG Conversion Factor"']
#NOTE: where "Combustion or WTT" & "Year" will be added as DEFAULT value.


ff_metadata = dict()    #; srcs = list(read_FFs(ss_dict))
for k,v in ff_dict.items():
        for file_id in v.keys():#range(len(v)):
                csv = open(f'{file_id}','r')
        
                FF_headers = (csv.readline()[:-1].split(','))
                #NOTE: [:-1] for the '\n'
        
                csv.close()
                if csv.closed:
                        pass
                        # print("File is now closed.")
        
                levels = FF_headers.index('UOM') - FF_headers.index('Level 1') #len(FF_headers)
                match levels:# {v[file_id]:schema_start + lvl_labels[:levels + `case ] + schema_end}
                        case 1: #NOTE: Homeworking
                                ff_dict[k][file_id] = schema_start + lvl_labels[:levels+1] + schema_end
                        case _ if levels > 1:
                                ff_dict[k][file_id] = schema_start + lvl_labels[:levels] + schema_end
# print(list(ff_dict.keys())[0].split('/'))
# print(list(list(ff_dict.values())[0][0].keys())[0].split('/'))#[1].strip())

# with open('cond_cols.json','w') as head_file:
        # js.dump(ff_dict,head_file,indent = 1)

"""
ff_dict ={src_dir:[src_main,src_facts]}
if (len(ff_dict[src_dir][0] + ff_dict[src_dir][0]) == N: #match
        l4 in main
        case N2:
                l4 in main, last_l
        case N3:
                last_l
        else:
                pass;)>> in src_INSERT.sql(s) inside its dir

# """
# with open("s.json",'x') as sfile:
#         js.dump(ff_dict,sfile,indent = 1)