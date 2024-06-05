select
    e.area_avaliacao as "Área de Avaliação"
  , e.mod_ens as "Modalidade Ensino"
  , round(mean(e.conc_enade_cont), 2) as "Conceito Enade Contínuo"
  , round(mean(e.conc_enade_faixa), 2) as "Conceito Enade Faixa"
from
  "./data/processed/enade.parquet" as e
where
  e.ano = 2021
group by
    e.area_avaliacao
  , e.mod_ens
order by
    e.area_avaliacao asc;
