select
    e.area_avaliacao
  , e.mod_ens
  , avg(conc_enade_cont) as avg_conc_enade_cont
from
  "../data/processed/enade.parquet" as e
group by
    e.area_avaliacao
  , e.mod_ens
order by
  avg_conc_enade_cont desc;
