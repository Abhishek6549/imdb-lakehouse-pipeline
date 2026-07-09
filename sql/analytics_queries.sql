-- 1. Top 10 highest-rated movies of each decade (min 1000 votes)
--    category (title_type) + time-series (decade) partition pruning both apply.
SELECT decade, primary_title, average_rating, num_votes
FROM
(
    SELECT
        decade,
        primary_title,
        average_rating,
        num_votes,
        row_number() OVER (PARTITION BY decade ORDER BY average_rating DESC) AS rnk
    FROM imdb.titles
    WHERE title_type = 'movie' AND num_votes >= 1000
)
WHERE rnk <= 10
ORDER BY decade DESC, average_rating DESC;

-- 2. Rating trend over time by title type (time-series analysis)
SELECT
    title_type,
    decade,
    round(avg(average_rating), 2) AS avg_rating,
    count() AS title_count
FROM imdb.titles
WHERE has_rating = 1 AND decade != 0  -- 0 = unknown/missing start_year
GROUP BY title_type, decade
ORDER BY title_type, decade;

-- 3. Genre popularity and quality (uses the pre-aggregated rollup for
--    sub-second dashboard-style responses)
SELECT genre, sum(title_count) AS titles, round(avg(avg_rating), 2) AS avg_rating
FROM imdb.genre_decade_stats
GROUP BY genre
ORDER BY titles DESC
LIMIT 20;

-- 4. Best rated TV series, ranked by the average rating of their episodes
SELECT
    series_title,
    count() AS episode_count,
    round(avg(average_rating), 2) AS avg_episode_rating,
    sum(num_votes) AS total_votes
FROM imdb.episodes
WHERE average_rating IS NOT NULL
GROUP BY series_title
HAVING episode_count >= 10
ORDER BY avg_episode_rating DESC
LIMIT 20;

-- 5. Season-over-season rating trend for a specific series
--    (:series_tconst is a bind parameter, e.g. 'tt0903747' for Breaking Bad)
SELECT
    season_number,
    round(avg(average_rating), 2) AS avg_rating,
    count() AS episode_count
FROM imdb.episodes
WHERE parent_tconst = {series_tconst:String}
GROUP BY season_number
ORDER BY season_number;

-- 6. Runtime vs. rating correlation, bucketed
SELECT
    multiIf(
        runtime_minutes < 60, '<60m',
        runtime_minutes < 90, '60-90m',
        runtime_minutes < 120, '90-120m',
        runtime_minutes < 180, '120-180m',
        '180m+'
    ) AS runtime_bucket,
    round(avg(average_rating), 2) AS avg_rating,
    count() AS title_count
FROM imdb.titles
WHERE title_type = 'movie' AND has_rating = 1 AND runtime_minutes IS NOT NULL
GROUP BY runtime_bucket
ORDER BY avg_rating DESC;
