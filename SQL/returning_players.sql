SELECT 
    p.nameFirst,
    p.nameLast, 
    a.playerID,
    MAX(prev.yearID) as last_seen_year,
    {year} as return_year,
    ({year} - MAX(prev.yearID) - 1) as gap_years,
    CONCAT(MAX(prev.yearID) + 1, ' - ', {year} - 1) as gap_years_range
FROM appearances a
JOIN people p ON a.playerID = p.playerID
JOIN appearances prev ON a.playerID = prev.playerID
WHERE a.yearID = {year} 
  AND prev.yearID < {year}
GROUP BY a.playerID, p.nameFirst, p.nameLast
HAVING gap_years >= 1
ORDER BY gap_years DESC; 