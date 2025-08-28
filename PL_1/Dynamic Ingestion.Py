import json

import pandas as pd
from  pl_maestro import (
      Pandas_XT,
      header_dict
    #   ,
    #   Pandas_3T,
    #   Pandas_5T,
      )
# id generator = 'record_No' + 'Scope_No' + 'Sheet_No' + 'Table_No'
f_name = "ghg_2025_full_Copy"

def prepare_data(cls,id,scope,T_NO):
    cls.set_transit(id,scope,T_NO[0])
    # cls.maintain(,id)

    return cls
    

    # input("Confirm the transformation to continue SQL:")
    # sql_file = input(f"sql_file for {f_name}: ")
    # cls.get_lvls(sql_file)
    # cls.get_facts(sql_file)

with open('Schema_Manifest.json', 'r') as f:
    json_data = json.load(f)
    # print(json_data['Scope 1'][0]['Fuels.1'])

SCOPE = ''
c= "class"

def steady(scope,energy,sc):
    global c, SCOPE
    for src in sc.keys():
                # if input(f"{type(src)}: ") == 'n':
                #         continue
                title_metas = src.split('.')
                TITLE_NO = title_metas# + title_metas[-1]
                id_suffix = scope.split()[-1]+str(json_data[scope].index(sc)+1)+TITLE_NO[-1]
                print("TITLE_NO",id_suffix)
                        # print(id_s)
                print(f'TITLE_NO: {TITLE_NO}')
                if 'SECR' in title_metas[0].split():#TODO
                    energy = 'kWh'
                    print("/***************\n\nWARNING! change the `Combustion or WTT` to 'Other', then hit ENTER!\n\n/***************") 
                if (src.find('WTT') + 1):
                    print("/***************\n\nWARNING! set the `Combustion or WTT` to 'WTT', then hit ENTER!!\n\n/***************")
                else:
                    print("/***************\n\nWARNING! ensure the `Combustion or WTT` is 'Combustion', then hit ENTER!!\n\n/***************")

                        # f_name = json_data[scope][src]['leveler']
                c_range = sc[src]['col_range']
                r_range = sc[src]['row_range']
                print(c_range, r_range)#; exit(0)
                        # if r_range <= 2:
                        #     pass
                            # r_range += int(input(f"/*\n CAUTION! r_range of {TITLE_NO} could be improper\n*/ ++: "))
                print(f"current leveler: {f_name}")
                df_3 =Pandas_XT(f_name,
                                sc[src]['leveler'],
                                c_range,r_range,energy,TITLE_NO)
                c = prepare_data(df_3,id_suffix,scope,TITLE_NO)
                # if input("The dicts are ready to be transformed. (already?!)") != "already":
                yield (pd.DataFrame.from_dict(c.transit_df))
                        
                        # print(df_3.sheet_1.head())
                        # continue
# print("Check the filename!",f_name.split('\\')[-1])

for scope in json_data.keys():
    SCOPE = scope
    #  if scope.split()[1] == header_dict['Scope']:
    energy = 'CO2e'
    sheet_counter = 1
    for sc in json_data[scope]:
        s = list(steady(scope,energy,sc))
        # print(len((s)))
        try:
            c.transit_df = dict(pd.concat([d for d in s]  , ignore_index=True))
        except:
             continue
        c.transform(sheet_counter,SCOPE);   sheet_counter += 1