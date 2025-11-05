create or replace table 
  `ggqqnfx-mcd-mged-kafka.vj_test_reports.intr_franchise_hr_hist_er_US` as
select 
  concat(geid, "_", primary_job_title) as unique_id,
  geid,
  nsn,
  primary_job_title as job_id,
  employee_start_date as start_date,
  termination_date as end_date,
  termination_reason as reason
from 
  `ggqqnfx-mcd-mged-kafka.vj_test_reports.stg_franchise_hr_hist_er_US`
where
  geid is not NULL
  and nsn is not NULL
  and primary_job_title is not NULL
  and employee_start_date is not NULL