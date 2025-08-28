># ` tree /F /a`
>Folder PATH listing for volume Work
>Volume serial number is `<ABC-123>`
```
<Drive_Letter>:.
|   .gitignore
|   2- Next stps.txt
<!-- of the 2nd pipeline. -->
|   Dynamic Ingestion.Py
<!-- The main function of the pipeline execution. -->
|   ghg_2025_full_Copy.xlsx
<!-- the root file of the GHG factors. -->
|   JOINs.sql
<!-- The channel to the target schema. -->
|   pipline1_outs.zip
<!-- a ZIP file containing all the CSV files as organized in their dirs. -->
|   PL_file_structure.py
<!-- Creates the folder tree of the scopes with their emission sources. -->
|   pl_maestro.py
<!-- The core (processor) of the PipeLine. -->
|   README.md
|   requirements.txt
|   Schema_Extraction.py
<!-- JSON writer of the tables metadat. -->
|   Schema_Manifest.json
<!-- {
 "<Scope N>": [
  {
   "<em_src.M>": {
    "leveler": "3T",
    "col_range": "A:G",
    "row_range": [
     <end_id>,
     <start_id>
    ]
   } -->

|   Steps.pdf
<!-- the flow diagram using `mermaid` -->
|   
     
+---Scope 0
|   +---[WTT- ]em_src_A
|   |       main_sheet.csv
|   |       UF_sheet.csv
|   |       
|   \---[WTT- ]em_src_Z
|           main_sheet.csv
|           UF_sheet.csv
|
...
|           
\---Scope N
    +---[WTT- ]em_src_A
    |       main_sheet.csv
    |       UF_sheet.csv
    |       
    \---[WTT- ]em_src_Z
            main_sheet.csv
            UF_sheet.csv
<!-- the UF_sheet.csv contains the the `Level 4`/`Level 5` column. -->
```            
