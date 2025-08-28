import json as js
import pandas as pd
from pyparsing import alphas



CONSTANTS = ('Activity','factors','FAQs','Other')
#NOTE: these are the blueprints of determining the tables on row-wise basis.

SCOPES = []
SOURCES = []
parent_file = "ghg_2025_full_Copy.xlsx"
INDEX_FRAME = pd.read_excel(parent_file,sheet_name="Index")

excel_col_ranges = dict(enumerate(range(1000)))
upper_alphas = alphas[:len(alphas)//2]
# for num in range(100):
# # singular char
num = 0

for char1 in upper_alphas:
        excel_col_ranges[num] = char1
        num += 1
        # continue
# double chars
for char1 in upper_alphas:
    for char2 in upper_alphas:
        excel_col_ranges[num] = char1 + char2
        num += 1

def S_S():
    global SCOPES, SOURCES
    sources_start = 0
    sources_end = -1
    # if (isinstance(SCOPES,tuple)):
###NOTE: This was JUST CAUSE for my prompt_trial.
    #     return dict({i:v for i,v in zip(SCOPES,SOURCES)})
    for val_id in range(len(INDEX_FRAME.iloc[:,0])):
        value = str(INDEX_FRAME.iloc[val_id,0])
        if value.split()[-1] == 'factors':
            # print(value)
            if len(SCOPES):
                sources_end = val_id - 1
                SOURCES.append(list(INDEX_FRAME.iloc[sources_start:sources_end,0])) 
            sources_start = val_id + 1
            SCOPES.append("".join(value[:7]))
        elif value == "Other":
        #NOTE: This is the '\0' of scopes.
            SOURCES.append(list(INDEX_FRAME.iloc[sources_start:(val_id-1),0])) 
            break
    # input("This page is NOT!")

    SCOPES = tuple(SCOPES); SOURCES = tuple(SOURCES)
    return dict({i:v for i,v in zip(SCOPES,SOURCES)})


# if __name__ == "__main__":
#     while(input("Anything else?") != 'n'):
#         try: #NOTE: hehehe NICE TRY 😎 👌 instead of overwhelming writes to the drive of saving new modifications.
#             eval(input("\nJust one-line command is allowed: \n"))
#         except:
#             print("Wrong syntax!\n")


src_reader = lambda s: pd.read_excel(parent_file, sheet_name=s)
#def outer_src"""scope: the dict containing these dicts""" 
# TODO
#for s in srcs:
#...
def src_config(src:str):
    """pd.read_excel(..., sheetname = `src`)"""
    SOURCE_FRAME = src_reader(src)
    SOURCE_WIDTH = SOURCE_FRAME.shape[1]
    last_cols =[]
    last_rows = []#NOTE: first element is the first_row
    col_start = ''
    mangle_title_s = []#fa.columns[-1].split('.')[0]
    levels = []

    sources_start = 0
    sources_end = -1
    if "FAQs" not in list(SOURCE_FRAME.iloc[:,0]):
        input(f"This {src} does NOT have the '\\0'!")

    for val_id in range(len(SOURCE_FRAME.iloc[:,0])):
        value = str(SOURCE_FRAME.iloc[val_id,0])
        if value == 'Activity':
            if len(last_cols):
                sources_end = val_id - 3 #NOTE: whether 5T or 3T, they FORTUNATELY leave fixed number of rows.
                # last_rows.append(sources_end - sources_start)
                last_rows.append([sources_end, sources_start]) 

            n_of_hdrs = len([h for h in list(SOURCE_FRAME.iloc[val_id,:]) if not pd.isna(h)])
#NOTE: SOURCE_WIDTH does NOT guarantee the symmetry of tables along the SAME source.
            last_cols.append(f"{excel_col_ranges[0]}:{excel_col_ranges[n_of_hdrs-1]}")
            
            if len([val for val in SOURCE_FRAME.iloc[val_id-1,:] if len(str(val)) > 3]): #NOTE:str(SOURCE_FRAME.iloc[val_id-1,X]) != 'nan': not that easy to predict 😎
                print(list(SOURCE_FRAME.iloc[val_id-1,:]))
                levels.append('5T')
                sources_start = val_id 
            else:
                levels.append('3T')
                sources_start = val_id + 1
            mangle_title_s.append(src+f'.{len(levels)}')

        elif value == "FAQs":
        #NOTE: This is the '\0' of scopes.
            last_rows.append([(val_id-2), sources_start])  
            break
    if not (len(levels)):
        input(f"{src} is empty!")
    return {t:
                                 {
                                      "leveler": lvl,
                                      "col_range":l_cs,
                                      "row_range":l_rs} 
                            for (t,lvl,l_cs,l_rs) in zip(mangle_title_s,levels,last_cols,last_rows)}
    # dict({i:v for i,v in zip(SCOPES,SOURCES)})


ss_dict = S_S()

def json_dumper():
    for v in ss_dict[k]:
        yield src_config(v)



if __name__ == "__main__":
    for k in ss_dict.keys():
        ss_dict[k] = list(json_dumper())
    with open("Schema_Manifest.json",'w') as js_file:
        js.dump(ss_dict,js_file,indent=1)
