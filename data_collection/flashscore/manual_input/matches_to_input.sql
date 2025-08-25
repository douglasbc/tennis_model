SELECT DISTINCT
    match_id,
    t.tournament_date,
    m.tournament_name,
    m.tournament_country,
    match_date,
    p1_name,
    p2_name,
    round
FROM analytics_layer.matches_atp AS m
JOIN treated_layer.atp_tournaments AS t ON t.tournament_id = m.tournament_id
WHERE
    (
        m.tournament_tier LIKE '%ATP%'
        OR m.tournament_tier = 'Masters 1000'
    )
    AND match_date BETWEEN '2022-01-01' AND '2023-12-28'
--     AND match_id NOT IN (
--     '42b2d9d460cc602379b396a15ee72f95',
--     'aa38b38943bc53404a9287c1503765c8',
--     'a891d51bbac335339d151020ccf88f6d',
--     'be8c546e17d768346cf4d96cc3e01065'
--     )
ORDER BY tournament_date, m.tournament_name, match_date, p1_name
