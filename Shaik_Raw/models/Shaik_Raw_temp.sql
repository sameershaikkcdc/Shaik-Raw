{{ config(materialized='table') }}

with 
source_data as (
	SELECT
		* 
		{{ dbt_utils.generate_surrogate_key(['Name','Roll_No']) }} as testing_srgt_key
		from {{ source('dmn01_finsoi_bqd_sameer','Sameer_Institute') }} raw
),

validated_data as (
	SELECT 
		*,
        [
            IF( SAFE_CAST(coalesce(Name,'0000') AS string) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("name" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",Name,"]") as error)),
            IF( SAFE_CAST(coalesce(course,'0000') AS string) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("branch" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",course,"]") as error)),    
            IF( SAFE_CAST(coalesce(Roll_No,'0000') AS int64) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("Roll_No" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",Roll_no,"]") as error)),
            IF( SAFE_CAST(coalesce(Place,'0000') AS string) is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("Place" as col,  CONCAT("dtypeCheckFailed [invalid string] [value: ",Place,"]") as error)),
            IF( SAFE_CAST(coalesce(year,'01/01/1001') AS DATE FORMAT 'DD/MM/YYYY') is NOT NULL,
                STRUCT('NA' as col, 'NA' as error),STRUCT("year" as col,  CONCAT("dtypeCheckFailed [invalid date] [value: ",year,"]") as error)),
	] as validation_reason
	from source_data 
),

error_data as (
	SELECT testing_srgt_key,
		ARRAY_AGG(error) as failure_reason,
       from validated_data, unnest(validation_reason) as error
             where error.col != 'NA'
       group by testing_srgt_key
)

select
	Name,
	course,
	Roll_No,
	year
from source_data sd left join error_data ed 
on sd.testing_srgt_key= ed.testing_srgt_key
