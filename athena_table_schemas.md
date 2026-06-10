# Athena table schemas (dev)

Reference schemas for the validation query (`cmdty_crgo_pln_leg_validation.sql`).
Both tables are partitioned; date filtering is done on `air_wb_cre_dt`
(target) / `airwaybillcreationdate` (source), not year/month/day columns.

## CMDTY_CURATE — `cmdty_crgo_curated` (partitioned)

| Column | Type |
|---|---|
| event_name | string |
| event_created | string |
| id | string |
| event_fqcn | string |
| airwaybillprefix | string |
| airwaybillnumber | string |
| airwaybillcreationdate | string |
| airwaybillpiecenumber | string |
| isunknown | string |
| isdeleted | string |
| ishazmat | string |
| pieceweight | string |
| servicelevel | string |
| wabcategory | string |
| specialhandlingcodes | string |
| radioactivecategory | string |
| transportindex | string |
| planneditinerary | array\<struct\<flightleg:struct\<departureairportiatacode:string, arrivalairportiatacode:string, origindate:string, flightnumber:string, operationalcarrier:string, operationalsuffix:string, repeatnumber:string\>, flightlegtypeid:string\>\> |
| commodityscans | array\<struct\<flightleg:struct\<departureairportiatacode:string, arrivalairportiatacode:string, origindate:string, flightnumber:string, operationalcarrier:string, operationalsuffix:string, repeatnumber:string\>, trackinglocation:struct\<area:string, location:string, action:string\>, scantype:string, scantime:string, scaniatastationcode:string, userid:string, container:string, devicename:string\>\> |

## PLN_LEG — `cmdty_crgo_pln_leg` (partitioned)

| Column | Type |
|---|---|
| air_wb_prfx_id | string |
| air_wb_num | string |
| air_wb_cre_dt | date |
| air_wb_cre_h2si | string |
| air_wb_pce_num | smallint |
| opng_carr_cde | string |
| opng_flt_num | smallint |
| opng_flt_num_sufx_txt | string |
| flt_dep_dt | date |
| leg_orig_arpt_cde | string |
| leg_dest_arpt_cde | string |
| eff_fm_cent_tz | timestamp |
| pln_crgo_leg_seq_num | int |
| pln_max_crgo_leg_seq_num | int |
| pln_crgo_dep_ld_flag | smallint |
| pln_crgo_arr_unld_flag | smallint |
| cmdty_flt_leg_type_cde | string |
| latest_job_id | int |
