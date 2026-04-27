SELECT SUM(
cardinality(string_to_array(skill, ','))
)
FROM job_postings
WHERE skill IS NOT NULL;

SELECT COUNT(DISTINCT lower(btrim(s.skill_part)))
FROM job_postings jp
CROSS JOIN LATERAL unnest(string_to_array(jp.skill, ',')) AS s(skill_part)
WHERE jp.skill IS NOT NULL
AND btrim(s.skill_part) <> '';