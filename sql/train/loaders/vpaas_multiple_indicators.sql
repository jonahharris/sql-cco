BEGIN;
/* ========================================================================= **
**                     ________________  _____ ____    __                    **
**                    / ____/ ____/ __ \/ ___// __ \  / /                    **
**                   / /   / /   / / / /\__ \/ / / / / /                     **
**                  / /___/ /___/ /_/ /___/ / /_/ / / /___                   **
**                  \____/\____/\____//____/\___\_\/_____/                   **
**                                                                           **
** ========================================================================= **
**                         CROSS-COOCCURRENCE IN SQL                         **
** ========================================================================= **
** SQL-Based Cross-cooccurrence Recommender System                           **
** Copyright (C) Jonah H. Harris <jonah.harris@gmail.com>                    **
** All Rights Reserved.                                                      **
**                                                                           **
** Permission to use, copy, modify, and/or distribute this software for any  **
** purpose is subject to the terms specified in the License Agreement.       **
** ========================================================================= */

-- ========================================================================= --
-- == CONFIGURATION ======================================================== --
-- ========================================================================= --

-- UR Default
INSERT INTO cco_config VALUES (50, 500, 'long_view');

-- ========================================================================= --
-- == DATA-SPECIFIC LOADING ================================================ --
-- ========================================================================= --

DROP TABLE IF EXISTS raw_events;
CREATE TABLE raw_events (
 	viewer_id								VARCHAR,
  broadcaster_id          VARCHAR,
  viewer_age              INTEGER,
  viewer_gender           VARCHAR,
  viewer_longitude        DOUBLE PRECISION,
  viewer_latitude         DOUBLE PRECISION,
  viewer_lang             VARCHAR,
  viewer_country          VARCHAR,
  broadcaster_age         INTEGER,
  broadcaster_gender      VARCHAR,
  broadcaster_longitude   DOUBLE PRECISION,
  broadcaster_latitude    DOUBLE PRECISION,
  broadcaster_lang        VARCHAR,
  broadcaster_country     VARCHAR,
  duration                INTEGER,
  viewer_network          VARCHAR,
  broadcaster_network     VARCHAR,
  count                   BIGINT
);

.import --csv --skip 1 ./data/raw/athena-results.csv raw_events

/*
 * During feature analysis, it appears vPaaS "age in years" ranges between
 * [18, 250]. As such, limit this to the categorical ranges we found worked
 * well in RECON V1.
 */
UPDATE raw_events
   SET viewer_age = min(max(18, viewer_age), 65),
       broadcaster_age = min(max(18, broadcaster_age), 65);

/*
WITH
     base AS (
          SELECT (re.viewer_network || ':' || re.viewer_gender || ':' || re.viewer_age || ':' || re.viewer_lang || ':' || re.viewer_country) AS viewer_key,
                 broadcaster_id,
                 CAST(MAX(3600, re.duration) AS DOUBLE PRECISION) / MAX(500, re.count) AS view_time
            FROM raw_events re
           GROUP BY 1, 2
     ),
     ranked AS (
          SELECT viewer_key,
                 broadcaster_id,
                 view_time,
                 ROW_NUMBER() OVER (PARTITION BY b.viewer_key ORDER BY b.view_time DESC) AS rank
            FROM base b
     )
INSERT
  INTO cco_events
       (entity, indicator, target)
SELECT DISTINCT viewer_key,
       'long_view',
       broadcaster_id
  FROM ranked r
 WHERE r.rank <= 100
 ORDER BY 1, 2, 3;
*/

INSERT
  INTO cco_events
       (entity, indicator, target)
SELECT DISTINCT viewer_id,
       'long_view',
       broadcaster_id
  FROM raw_events r
 WHERE duration > 120
 ORDER BY 1, 2, 3;

INSERT
  INTO cco_events
       (entity, indicator, target)
SELECT DISTINCT viewer_id,
       'view',
       broadcaster_id
  FROM raw_events r
 WHERE duration >= 30
       AND duration < 120
 ORDER BY 1, 2, 3;

INSERT
  INTO cco_events
       (entity, indicator, target)
SELECT DISTINCT viewer_id,
       'click',
       broadcaster_id
  FROM raw_events r
 WHERE duration >= 5
       AND duration < 30
 ORDER BY 1, 2, 3;

/* :vi set ts=2 et sw=2: */
COMMIT;
