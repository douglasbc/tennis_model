{{ config(
    materialized = 'table',
    schema = 'treated_layer'
)}}

with seeds as (
  select * from {{ source('raw_layer', 'seeds_wta') }}
),

final as (
    select    
      player_id,
      tournament_id,
      case
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('A', 'ALT', 'Alt') 
        then 'Alternate'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('IR', 'ITF', 'P') 
        then 'ITF Ranking'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') = 'JE' 
        then 'Junior Exempt'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') = 'JR' 
        then 'Junior Reserved'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') = 'LL' 
        then 'Lucky Loser'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('PR', 'SR')
        then 'Special Ranking'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('SE', 'S') 
        then 'Special Exempt'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('W', 'WC')
        then 'Wild Card'
        when REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)') in ('q', 'Q') 
        then 'Qualifier' 
        else REGEXP_EXTRACT(seeding, r'([a-zA-Z]+)')
      end as entry_status,
      REGEXP_EXTRACT(seeding, r'([0-9]+)') as seed_number,

    from seeds
)

select * from final
