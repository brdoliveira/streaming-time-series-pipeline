SELECT time_bucket('1 second', processing_time) AS second,
       scenario,
       count(*) AS events
FROM financial_events
GROUP BY second, scenario
ORDER BY second DESC, scenario
LIMIT 120;
