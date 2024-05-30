select
    e.area_avaliacao
  , e.ies
  , dense_rank() over (
      order by
        e.conc_enade_cont desc
    ) as ranking_enade_cont
from
  "./data/processed/enade.parquet" as e
where
  e.area_avaliacao = 'ADMINISTRAÇÃO';
