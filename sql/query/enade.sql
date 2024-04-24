select
    area_avaliacao
  , mod_ens
  , mean(nota_bruta_fg) as nota_bruta_fg
  , mean(nota_padronizada_fg) as nota_padronizada_fg
  , mean(nota_bruta_ce) as nota_bruta_ce
  , mean(nota_padronizada_ce) as nota_padronizada_ce
  , mean(conc_enade_cont) as conc_enade_cont
from
  "../data/processed/enade.parquet"
group by
    area_avaliacao
  , mod_ens
order by
    area_avaliacao asc
  , mod_ens asc;
