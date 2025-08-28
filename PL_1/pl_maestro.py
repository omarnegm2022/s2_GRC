# #define constants, merge new_cols, separate cols to another sheet, skip rows & useRangeS, ffill_cells
# t date
"""Snippets conventions for good follow:
UAT: used at line, for commonly used vars along the script, or defined in a remote scope from its target.

"""
#NOTE
#Rule No.1 in this program: Atomicity
#NOTE

from datetime import date
import numpy as np
import pandas as pd
# from Schema_Extraction import ss_dict

#NOTE: s1_dumps is the python reader program of .json file 
# from Scope_1.s1_dumps import #
#GHG game
#NOTE(static): "id": int, "Scope": , Level4, Level5, Combustion or WTT, Level1, Year
#TODO(dynamic): Level2, Level3, UOM, GHG/Unit, GHG Conversion Factor

header_dict = {"Emissions source":	"Fuels"#Level1 == sheet_name
               ,	"Next publication date": 	date(month = 6, year= 2026,day=2),	"Factor set":	"Full set",
               "Scope":	1,	"Version":	1.0,	"Year":	2025}
"""
Activity --> Level2
Fuel --> Level3
Unit --> UOM
[kg CO2e,	kg CO2e of CO2 per unit,	kg CO2e of CH4 per unit,	kg CO2e of N2O per unit] --> GHG/Unit

INTERSECT(UOM, GHG/Unit) --> GHG Conversion Factor

"""

# DEFAULTS = {


# }
new_sheets = {'main':"sheet_1", 'factors':"UF_sheet"}


class Pandas_XT:
    def __init__(self,xsl_file,TYPE,src_range:str,data_range,energy:str,suffix:str) -> None:
        """
        `xsl_file`: file name without the extension type.
        `src_range`: is the table column range in the sheet, for example: A:D, BA:CE
        `data_range`: n_of_rows to be read. In case of some adjacent table has different number of rows, so we make sure the target one is read properly (otherwise we may have fully NULL records or missing records.)
        """

        #(header_dict['Emission source'].lower())
        #NOTE: this is just a function, will be used in the next 2 methods, explained afterwards...
        self.table_suffix = suffix #NOTE: It is a combination of the src(sheet_name) and its No.
# UAT: 59
        self.file_name = xsl_file

        esc_rows = 1
        self.sheet_1 = pd.read_excel(f"{xsl_file}.xlsx"
                          ,skiprows=esc_rows + data_range[1] - 1      #NOTE: where the first row is just the title of level1(the emission source)
                          ,nrows= data_range[0] - data_range[1]
                          ,usecols=src_range,

                          sheet_name= suffix[0]
                          )
        self.type = TYPE #NOTE: 3T or 5T
        if TYPE == '5T':
            self.f_lvls = [header for header in self.sheet_1.columns if not(header.find('Unnamed') + 1)] #NOTE: This row is skipped in the next step right away.
        #NOTE: the first row, interpreted by Pandas normally as the header of the DF, where empty cells are arbitrary named: 'Unnamed:N'
#TODO: this is definitely applicable to scope1, and probably to the other scopes

            self.sheet_1 = pd.read_excel(f"{xsl_file}.xlsx"
                          ,skiprows= esc_rows + data_range[1]
                          ,nrows= data_range[0] - data_range[1] - 1 #TODO: This is right, @AHMED?
                          ,usecols= src_range,
                          sheet_name= suffix[0]
                        #   !['xlsx_ranges'] [header_dict['Emission source'].lower()]
                          )        
        # self.sheet_1.columns = [field.split('.')[0] for field in self.sheet_1.columns]  #NOTE: That because Pandas at first implicitly reads the entire selected sheet, so to handle duplicate column names,
                                                                                                    # it adds a suffix as follows: col_name.r[0-9], for instance: Activity.2 for 3rd table as both of 2nd and 1st has the same name literally.

        self.facts = [field for field in list(self.sheet_1.columns) if (field.find(energy) + 1)] # + ['Unit']
        self.main_fields = [field for field in list(self.sheet_1.columns) if field not in self.facts]
        print( self.facts, self.main_fields)#; exit(0)
        self.transit_df = dict()

################################
#Reviwed by: Omar Saeed
################################
    def set_transit(self,id,scope,em_src):
        """
        `id`: its prefix from the config.json
        Order matters in adding the columns for appropriate mapping to INSERT command.
        """
        mangle_dupe_cols = [field.split('.')[0] 
                            for field in self.facts]
        #5t_proprietary: mangle_dupe_cols = [field.split('.')[0] for field in self.facts]
        for field in self.main_fields:#TODO:You can ignore this loop, @AHMED! You may find it also in `Pandas_5T` class
            self.sheet_1[field.lstrip()].ffill(inplace = True)

        # print(f"self.sheet_1.shape[0]:   {self.transit_df.shape[0]}")
        self.transit_df['index'] = np.array([int(id+str(i)) for i in range(self.sheet_1.shape[0])]*
                                                                                                   len(self.facts))#NOTE: because.. Shut up now!
        
        self.transit_df['Scope'] = np.array([[scope for _ in range(self.sheet_1.shape[0])] for _ in range(len(self.facts))]).flatten()
        self.transit_df['Level 1'] = np.array([[em_src for _ in range(self.sheet_1.shape[0])] for _ in range(len(self.facts))]).flatten()

        init_ranger = 1 if (self.main_fields[-1] != 'Year') else 2
        for f in range(len(self.main_fields)- init_ranger ): #NOTE: -1 for UOM 
            self.transit_df[self.main_fields[f]] = np.array([self.sheet_1[self.main_fields[f]] for _ in range(len(self.facts))]).flatten()

        if self.type == '5T':      
        #NOTE:at implementing PL_2, changed from dynamic naming to fixed one, feasible for smooth INSERT.                                                                                                    
            self.transit_df[f'Last level'] = np.array([[lvl for _ in range(self.sheet_1.shape[0] *
                                                                                             (
                                                                                              len(set(mangle_dupe_cols))
                                                                                              )
                                                                                             )] 
                                                                                             for lvl in self.f_lvls]).flatten()
        elif (len(self.main_fields) > 3):# and (self.main_fields[-1] != 'Year'):
            self.transit_df['Last level'] = np.array([self.sheet_1[self.main_fields[-2]] for _ in range(len(self.facts))]).flatten()

        self.transit_df['UOM'] = np.array([self.sheet_1['Unit'] for _ in range(len(self.facts))]).flatten()
         #NOTE: WATCH OUT for potential SUFFIX.

                                        
#NOTE: KEEP AN EYE OUT FOR id
        self.transit_df['GHG/Unit'] =  np.array([[                                         # I am unpivoting/flattening the GHG units.
            f.split('.')[0] #NOTE: recall the implicit suffix by Pandas of col names*
            for _ in range(self.sheet_1.shape[0])]    for f in self.facts]).flatten()
            
        self.transit_df['GHG Conversion Factor'] = np.array([self.sheet_1[f] for f in self.facts]).flatten()

#HUD: all mangles are one as collection 🙃
        
        # self.transit_df['UOM'] = np.array([u for u in self.sheet_1['Unit']])
        
        # self.transit_df.update({u:self.sheet_1[u] for u in self.facts
        #                                                             [:-1]#NOTE: -1 <- 'Unit'
        #                                                             })
        for i in self.transit_df.values():
            input(len(i))

        print(f"\n/*******\nNumber of factors: {len(self.transit_df.keys())-2}. \nReflect it to your veiw sql.\n/*******\n")
################################
#Reviwed by: Omar Saeed
################################    
    def transform(self,sheet_counter:int,scope):
        
        for i in self.transit_df.values():
            print(len(i))
        print("All equal ?")#NOTE: I mean all shapes are equal, like exception handling. just hit ENTER :D


        ##############################################################
        self.transit_df = pd.DataFrame.from_dict(self.transit_df)
            
            ##############################################################


        # try:
        #     1/(len(self.transit_df.keys()) - len($['main']['3T_main']['column_name']))#NOTE: when it gives division_by_zero error, so in this case it is correct XD. Yes, where the handling writes the new sheet.
        #     if (len(self.transit_df.keys()) - len($['facts']['3T_facts']['column_name'])
        #     ):
        #         print("\n\n* * *\nfacts != facts\n* * *\n\n")#TODO: from pg_schema through $.json
        #     input("main != main")#TODO: from pg_schema through $.json
        # except:

        #TODO:.ExcelWriter.param if_sheet_exists="new"
        # with pd.ExcelWriter(f"{self.file_name}3T.xlsx", engine='openpyxl', mode='a',if_sheet_exists='overlay') as writer:
        #     self.transit_df.to_excel(writer,sheet_name=new_sheets['main'] 
        #                         #   + self.table_suffix
        #                           ,index=False)
        self.transit_df.to_csv(f"{scope}/{sheet_counter}. {self.table_suffix[0]}.csv",index=False)
        # with pd.ExcelWriter(f"{self.file_name}3T.xlsx", engine='openpyxl', mode='a',if_sheet_exists='overlay') as writer:
        #     self.transit_df.to_excel(writer,sheet_name= new_sheets['factors'] 
        #                         #   + self.table_suffix
        #                           ,index=False)

    def view(self):
        """gets the number of the tables related to the same source"""
        return pd.read_excel(f"{self.file_name}.xlsx").columns
    #NOTE: just a vacancy method*
#TODO: By the way, @AHMED, you can change the name of `factorize()` and `maintain()`, to `set_factors()` and `set_lvls()`... just to follow OOP conventions, you get it ? =)



# #TODO: Sorry for that, @AHMED, but you may find ZOMBIE print() calls along the classes methods... which you find unnecessary, kindly delete them!
