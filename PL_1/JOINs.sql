-- View: public.5T_Source

-- DROP VIEW public."5T_Source";
/* 
1. In the Airflow code, we would just open(".csv",'r').readlines()[0], then

but first of all, convert the rohmbous-like symbol to `-`. Along the ALL lines normally 
YOU KNOW WHAT?! It is the Microsoft JACKASS. 
As the content is read correctly in both notepad and when importing to pgAdmin.
  (directly the scope 3 facts_sheet(s)).
--Leave Pandas honored for the unique PL_1. ;D

.1 if 'Level 4' in main_0:
    "main".*, "main"."Level 4",
    "facts"."last_level" as var_5
  else:# It was FORBIDDEN by NumPy to allow such level to be flattened in the main.
    "main".*,
    "facts"."last_level" as var_4
where: var_4, var_5 are pyVars of '"Level 4"', '"Level 5"' respectively

2. open() in one is along a `for loop`, so:
    if file_name.split('-') is WTT:
      set the `%::text` line first token: 'WTT'
    elif file_name.split() is SECR:
      set the `%::text` line first token: 'Other'

[1,2] are for the JOIN work item in the DAG.
---
3. from 1 import var_4, var_5 as booleans
  match (var_4 + var_5):
    case 2:
      add 2 params
    case 1:
      add 1 param (intuitively: Level 4)
[3] is for the Loading the data into the target DB, work item2 of the DAG.
---
#. Ashour's Backup script for the last correct table as failover to.
[#] This will have another part in the same DAG, or ELSE. << Last thing to integrate after testing
WI(1,2) of the main DAG.

*/
CREATE OR REPLACE VIEW public."Source"
 AS
 SELECT main.main_id AS id,
    main."Scope",
    main."Level 1",
    main."Level 2",
    main."Level 3",
    'Combustion'::text AS "Combustion or WTT",
    facts."Last_lvl",
    facts."GHG/Unit",
    facts."GHG Conversion Factor"
   FROM main
     JOIN facts ON facts.factor_id = main.main_id;

ALTER TABLE public."Source"
    OWNER TO postgres;

