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

.mode column
.headers on

/*
 * As downsampling requires randomness and random() isn't stable within a CTE,
 * we need to dump the downsampled data set to a temporary table for use in
 * subsequent analysis.
 *
 * NOTE: While both SQLite and Postgres claim to support the AS MATERIALIZED
 *       clause for CTEs, whether materialization is actually performed is up
 *       to the optimizer. Accordingly, while it sucks to waste the additional
 *       space, failing to take randomness correctly into consideration causes
 *       massive badness here. So, let's just stay safe out there.
 */
WITH
  -- pull our configuration in for subsequent queries
  config
  AS (SELECT max_related_count,
             max_interaction_count,
             primary_indicator
        FROM cco_config
       LIMIT 1
     ),
  -- select primary indicator events
  events
  AS (SELECT i.indicator_id,
             e.entity_id,
             t.target_id
        FROM cco_events ev
        JOIN cco_entities e
             ON (e.entity = ev.entity)
        JOIN cco_indicators i
             ON (i.indicator = ev.indicator)
        JOIN cco_targets t
             ON (t.target = ev.target)
       --WHERE ev.indicator = (SELECT primary_indicator FROM config)
       --WHERE ev.indicator = 'purchase'
       ORDER BY 1, 2, 3
     ),
  -- compute A
  user_items
  AS (SELECT indicator_id,
             entity_id AS row_num,
             target_id AS col_num,
             1 AS value
        FROM events e
       ORDER BY 1, 2
     ),
  -- apply selective downsampling of users/items with an anomalous number of interactions
  downsample_start
  AS (SELECT indicator_id,
             row_num,
             col_num,
             value,
             COUNT() OVER (PARTITION BY indicator_id, row_num) AS interactions_in_row
             --COUNT() OVER (PARTITION BY row_num) AS interactions_in_row
        FROM user_items
     ),
  downsample_vars
  AS (SELECT indicator_id,
             row_num,
             col_num,
             value,
             interactions_in_row,
             (CAST(min((SELECT max_interaction_count FROM config), interactions_in_row) AS REAL) / interactions_in_row) AS per_row_sample_rate,
             (CAST(min((SELECT max_interaction_count FROM config), value) AS REAL) / value) AS per_thing_sample_rate,
             (CAST(random() AS REAL) / 4611686018427387904 / 4 + 0.5) AS randval
        FROM downsample_start
     ),
  downsampled_user_items
  AS (SELECT indicator_id,
             row_num,
             col_num,
             1 AS value
        FROM downsample_vars
       WHERE randval <= min(per_row_sample_rate, per_thing_sample_rate)
     )
--SELECT * FROM user_items;
--SELECT * FROM downsample_start;
--SELECT * FROM downsample_vars;
--SELECT * FROM downsampled_user_items;
INSERT
  INTO cco_downsampled_events
       (entity_id, indicator_id, target_id)
SELECT row_num, indicator_id, col_num
  FROM downsampled_user_items
 ORDER BY 1, 2, 3;

/* :vi set ts=2 et sw=2: */
COMMIT;
