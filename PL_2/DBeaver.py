import os
import json as js
from FF_prepare import ff_dict
"""
INERT INTO my_s2.main 
(    main_id ,
    "Scope" ,
    "Level 1",
    "Level 2",
    "Level 3",
    "Level 4",
    "UOM" 
    ) VALUES \n

*/
		D 		E 		P 		R 		E 		C 		A 		T 		E 		D 			=)
																									/*
INSERT INTO my_s2.facts
(
    fact_id,
    "Last_lvl",
    "GHG/Unit",
    "GHG Conversion Factor"
) VALUES \n
"""

# f"""{'n_l, '.join(map(str, [tuple(i) for i in tuple(f_vals)]))};"""

def sql_dump():
	for src,v in ff_dict.items():
		q_mode = 'x'
		# with open(f"{src}/{src.split('/')[2]}.sql",'x') as scope_dml: #create & write
			# cols(main_schema.copy(), facts_schema.copy())
		for file_path,schema in v.items():
			query_p1 = f"""INSERT INTO "{src.split('/')[2]}" ({', '.join(map(str, schema))})"""
			print('\n',file_path.split('.')[1].strip(),'\n')
				# print(query_p1)
			records = []
			with open(file_path,'r') as rec:
				for line in rec:
					records.append(line[:-1].strip().split(','))

			query_p2 = f"""n_l VALUES {'n_l, '.join(map(str, [tuple(i) for i in tuple(records)] ))};""".replace('n_l','\n').replace(", ''",", NULL")#.replace(", '')",", NULL)")
					#NOTE: as the ESC chars are not valid in f-string.

			# print(query_p1 + query_p2)
			with open(f"{src}/{src.split('/')[2]}.sql", q_mode) as scope_dml: #create & write
				scope_dml.write(f'\n-- {file_path}\n')
				scope_dml.writelines(query_p1 + query_p2)
				print(os.listdir(f"{src}/")[-1]," created.")
				q_mode = 'a'
		# break


		# scope_dml.writelines()