select
    e.area_avaliacao
  , e.mod_ens
  , round(mean(e.nota_bruta_fg), 2) as nota_bruta_fg
  , round(mean(e.nota_padronizada_fg), 2) as nota_padronizada_fg
  , round(mean(e.nota_bruta_ce), 2) as nota_bruta_ce
  , round(mean(e.nota_padronizada_ce), 2) as nota_padronizada_ce
  , round(mean(e.conc_enade_cont), 2) as conc_enade_cont
from
  "./data/processed/enade.parquet" as e
where
  e.ano = 2021
group by
    e.area_avaliacao
  , e.mod_ens
order by
    e.area_avaliacao asc;
