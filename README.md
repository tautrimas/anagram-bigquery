# BigQuery SQL-only solution to Trustpilot challenge

1. Upload dictionary as a table `anagrams.dictionary_full` to Google Cloud Platform BigQuery.
1. Execute the following documented query.
1. Running time is ~8 seconds.

```sql
-- Subtracts sets. Eg. sets_subtract(['a', 'a', 'b', 'c'], ['a', 'b']) returns ['a', 'c']. Returns NULL, if sets don't overlap.
CREATE TEMP FUNCTION sets_subtract(minuend ARRAY<STRING>, subtrahend ARRAY<STRING>) AS 
(
  CASE WHEN (
    -- Return NULL, if subtrahend contains characters not present in minuend
    SELECT COUNT(t1) < ARRAY_LENGTH(subtrahend)
    FROM (
      SELECT t1
      FROM (SELECT t1, row_number() OVER (PARTITION BY t1) rn FROM UNNEST(subtrahend) AS t1) t1
      JOIN (SELECT s1, row_number() OVER (PARTITION BY s1) rn FROM UNNEST(minuend) AS s1) s1 ON s1.s1 = t1.t1 AND s1.rn = t1.rn
    )
  ) THEN NULL 
  ELSE (
    -- Select remaining characters not present in subtrahend
    ARRAY(
      SELECT s1.s1
      FROM (SELECT s1, row_number() OVER (PARTITION BY s1) rn FROM UNNEST(minuend) AS s1) s1
      LEFT JOIN (SELECT t1, row_number() OVER (PARTITION BY t1) rn FROM UNNEST(subtrahend) AS t1) t1 ON s1.s1 = t1.t1 AND s1.rn = t1.rn
      WHERE t1.t1 IS NULL
    )
  ) 
  END
);

-- Load the dictionary into a table `anagrams.dictionary_full` before starting.

WITH
-- Dictionary contains duplicates, remove them.
dictionary_unique AS (
  SELECT DISTINCT word FROM `anagrams.dictionary_full`
),
-- For optimisation, remove useless words based on not full overlap with 'poultryoutwitsants'
dictionary AS (
  SELECT 
    word, 
    SPLIT(word, '') word_array 
  FROM dictionary_unique 
  WHERE sets_subtract(SPLIT('poultryoutwitsants', ''), SPLIT(word, '')) IS NOT NULL
),
-- Select the first word and calculate the remainder
level1 AS (
  SELECT sets_subtract(SPLIT('poultryoutwitsants', ''), d1.word_array) remainder, d1.word word1
  FROM dictionary d1
),
-- Select the second word and calculate the remainder.
-- Remainder is normalized so that we can JOIN on it directly for words 3 and 4
level2 AS (
  SELECT
    -- Split string into chars, order by value, glue back to a string
    array_to_string(ARRAY(SELECT c FROM UNNEST(sets_subtract(level1.remainder, d1.word_array)) AS c ORDER BY c), '') remainder_normalized,
    level1.word1, 
    d1.word word2
  FROM 
    level1,
    dictionary d1
  WHERE sets_subtract(level1.remainder, d1.word_array) IS NOT NULL
),
-- Build normalized dictionary for joining.
dict_normalized AS (
  SELECT d.word, array_to_string(ARRAY(SELECT c FROM UNNEST(d.word_array) AS c ORDER BY c), '') normalized
  FROM dictionary d
),
-- Join the 3rd word only when normalized remainder equals normalized dictonary word.
-- SQL logic ensures that all combinations of distinct word before normalization are considered later.
-- Using an equality JOIN with dictionary, only 1 word can be joined, so this table contains only full 3 word phrases.
level3 AS (
  SELECT level2.word1, level2.word2, dn.word word3
  FROM level2, dict_normalized dn 
  WHERE level2.remainder_normalized = dn.normalized
),
-- In order to consider 4 word phrases, just join dictionary with dictionary itself 
-- so that we have all 3rd and 4th word combinations.
level34 AS (
  SELECT 
    dn3.word word3, 
    dn4.word word4,
    -- Store which letters were used in both words for JOIN later
    array_to_string(ARRAY(SELECT c FROM UNNEST(SPLIT(CONCAT(dn3.word, dn4.word), '')) AS c ORDER BY c), '') w3w4_normalized
  FROM dict_normalized dn3
  CROSS JOIN dict_normalized dn4
)

-- Collect results

-- Consider two word phrases
SELECT
  l.word1, 
  l.word2,
  NULL as word3,
  NULL as word4,
  to_hex(md5(CONCAT(l.word1, ' ', l.word2))) `md5`
FROM level2 l
WHERE l.remainder_normalized = ''
  AND to_hex(md5(CONCAT(l.word1, ' ', l.word2))) IN
    ('e4820b45d2277f3844eac66c903e84be', '23170acc097c24edb98fc5488ab033fe', '665e5bcb0c20062fe8abaaf4628bb154')
    
-- Consider three word phrases
UNION ALL
SELECT 
  l.word1, 
  l.word2, 
  l.word3, 
  NULL word4, 
  to_hex(md5(CONCAT(l.word1, ' ', l.word2, ' ', l.word3))) `md5`
FROM level3 l
WHERE to_hex(md5(CONCAT(l.word1, ' ', l.word2, ' ', l.word3))) IN
  ('e4820b45d2277f3844eac66c903e84be', '23170acc097c24edb98fc5488ab033fe', '665e5bcb0c20062fe8abaaf4628bb154')
  
-- Consider four word phrases
UNION ALL
SELECT 
  l2.word1, 
  l2.word2, 
  l34.word3, 
  l34.word4, 
  to_hex(md5(CONCAT(l2.word1, ' ', l2.word2, ' ', l34.word3, ' ', l34.word4))) `md5`
FROM level2 l2
-- Join previous 2 words remainder with 3rd + 4th words remainder on equality
JOIN level34 l34 ON l2.remainder_normalized = l34.w3w4_normalized
WHERE to_hex(md5(CONCAT(l2.word1, ' ', l2.word2, ' ', l34.word3, ' ', l34.word4))) IN
  ('e4820b45d2277f3844eac66c903e84be', '23170acc097c24edb98fc5488ab033fe', '665e5bcb0c20062fe8abaaf4628bb154')
```

Result:

<img width="537" alt="Screenshot of BigQuery results table" src="https://user-images.githubusercontent.com/364223/110109903-b9d20200-7db6-11eb-8173-f26f2e051280.png">
