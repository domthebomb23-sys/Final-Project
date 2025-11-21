SELECT disaster, COUNT(*) as occurrences
FROM disasters
GROUP BY disaster
ORDER BY occurrences DESC;