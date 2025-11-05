create or replace table  
    `ggqqnfx-mcd-mged-kafka.vj_test_reports.dim_franchise_hr_hist_US` as
select 
    *
from 
    `ggqqnfx-mcd-mged-kafka.vj_test_reports.intr_franchise_hr_hist_er_US`
union all
select
    *
from
    `ggqqnfx-mcd-mged-kafka.vj_test_reports.intr_franchise_hr_hist_harri_US`
union all
select
    *
from
    `ggqqnfx-mcd-mged-kafka.vj_test_reports.intr_franchise_hr_hist_lf_US`
