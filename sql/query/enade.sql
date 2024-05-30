select
    e.area_avaliacao
  , e.mod_ens
  , mean(e.nota_bruta_fg) as nota_bruta_fg
  , mean(e.nota_padronizada_fg) as nota_padronizada_fg
  , mean(e.nota_bruta_ce) as nota_bruta_ce
  , mean(e.nota_padronizada_ce) as nota_padronizada_ce
  , mean(e.conc_enade_cont) as conc_enade_cont
from
  "./data/processed/enade.parquet" as e
group by
    e.area_avaliacao
  , e.mod_ens
order by
    e.area_avaliacao asc
  , e.mod_ens asc;
