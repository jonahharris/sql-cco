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

WITH
  -- pull our configuration in for subsequent queries
  config
  AS (SELECT max_related_count,
             max_interaction_count,
             primary_indicator
        FROM cco_config
       LIMIT 1
     ),
  -- select primary indicator events (A)
  primary_user_items
  AS (SELECT entity_id AS row_num,
             target_id AS col_num
        FROM cco_downsampled_events
       WHERE indicator_id = (SELECT indicator_id
                               FROM config
                               JOIN cco_indicators
                                    ON (indicator = primary_indicator))
       ORDER BY 1, 2
     ),
  secondary_user_items
  AS (SELECT entity_id AS row_num,
             target_id AS col_num,
             indicator_id AS indicator_id
        FROM cco_downsampled_events
       WHERE indicator_id <> (SELECT indicator_id
                                FROM config
                                JOIN cco_indicators
                                     ON (indicator = primary_indicator))
       ORDER BY 1, 2, 3
     ),
  primary_interaction_counts
  AS (SELECT col_num,
             COUNT(*) AS interaction_count
        FROM primary_user_items
       GROUP BY 1
     ),
  secondary_interaction_counts
  AS (SELECT indicator_id,
             col_num,
             COUNT(*) AS interaction_count
        FROM secondary_user_items 
       GROUP BY 1, 2
     ),
  -- compute the cooccurrence matrix A'B
  at_b_sql
  AS (SELECT a1.indicator_id,
             a2.col_num AS row_num,
             a1.col_num AS col_num,
             COUNT(*) as value
        FROM secondary_user_items a1
        JOIN primary_user_items a2
             ON (a1.row_num = a2.row_num)
       GROUP BY a1.indicator_id, a1.col_num, a2.col_num
     ),
  llr_input_ut
  AS (SELECT ii.indicator_id,
             ii.row_num,
             ii.col_num,
             SUM(ii.value) AS nb_interactions_with_a_b,
             a.interaction_count AS nb_interactions_with_a,
             b.interaction_count AS nb_interactions_with_b,
             (SELECT COUNT(DISTINCT row_num) FROM primary_user_items) AS nb_interactions
        FROM at_b_sql ii
        JOIN primary_interaction_counts a
             ON (a.col_num = ii.row_num)
        JOIN secondary_interaction_counts b
             --ON (b.col_num = ii.col_num)
             --ON (b.indicator_id = ii.indicator_id AND b.col_num = ii.row_num)
             ON (b.indicator_id = ii.indicator_id AND b.col_num = ii.col_num)
       --GROUP BY ii.indicator_id, ii.col_num, ii.row_num
       GROUP BY ii.indicator_id, ii.row_num, ii.col_num
     ),
  llr_base
  AS (SELECT indicator_id,
             col_num,
             row_num,
             nb_interactions_with_a_b AS k11,
             (nb_interactions_with_a - nb_interactions_with_a_b) AS k12,
             (nb_interactions_with_b - nb_interactions_with_a_b) AS k21,
             (nb_interactions - nb_interactions_with_a - nb_interactions_with_b + nb_interactions_with_a_b) AS k22
        FROM llr_input_ut
     ),
  llr_step_one
  AS (SELECT indicator_id,
             col_num,
             row_num,
             k11,
             k12,
             k21,
             k22,
             CASE WHEN k11 = 0 THEN 0
                  ELSE (k11 * ln(k11))
             END AS xlog_k11,
             CASE WHEN k12 = 0 THEN 0
                  ELSE (k12 * ln(k12))
             END AS xlog_k12,
             CASE WHEN k21 = 0 THEN 0
                  ELSE (k21 * ln(k21))
             END AS xlog_k21,
             CASE WHEN k22 = 0 THEN 0
                  ELSE (k22 * ln(k22))
             END AS xlog_k22
        FROM llr_base
     ),
  llr_step_two
  AS (SELECT indicator_id,
             col_num,
             row_num,
             k11,
             k12,
             k21,
             k22,
             xlog_k11,
             xlog_k12,
             xlog_k21,
             xlog_k22,
             CASE WHEN ((k11 + k12) = 0)
                  THEN (0.0 - xlog_k11 - xlog_k12)
                  ELSE (((k11 + k12) * ln(k11 + k12)) - xlog_k11 - xlog_k12)
             END AS entropy_k11_k12,
             CASE WHEN ((k21 + k22) = 0)
                  THEN (0.0 - xlog_k21 - xlog_k22)
                  ELSE (((k21 + k22) * ln(k21 + k22)) - xlog_k21 - xlog_k22)
             END AS entropy_k21_k22,
             CASE WHEN ((k11 + k21) = 0)
                  THEN (0.0 - xlog_k11 - xlog_k21)
                  ELSE (((k11 + k21) * ln(k11 + k21)) - xlog_k11 - xlog_k21)
             END AS entropy_k11_k21,
             CASE WHEN ((k12 + k22) = 0)
                  THEN (0.0 - xlog_k12 - xlog_k22)
                  ELSE (((k12 + k22) * ln(k12 + k22)) - xlog_k12 - xlog_k22)
             END AS entropy_k12_k22,
             CASE WHEN ((k11 + k12 + k21 + k22) = 0)
                  THEN (0.0 - xlog_k11 - xlog_k12 - xlog_k21 - xlog_k22)
                  ELSE (((k11 + k12 + k21 + k22) * ln(k11 + k12 + k21 + k22))
                        - xlog_k11 - xlog_k12 - xlog_k21 - xlog_k22)
             END AS entropy_k11_k12_k21_k22
        FROM llr_step_one
     ),
  llr_step_three
  AS (SELECT indicator_id,
             col_num,
             row_num,
             k11,
             k12,
             k21,
             k22,
             xlog_k11,
             xlog_k12,
             xlog_k21,
             xlog_k22,
             entropy_k11_k12,
             entropy_k21_k22,
             entropy_k11_k21,
             entropy_k12_k22,
             entropy_k11_k12_k21_k22,
             (entropy_k11_k12 + entropy_k21_k22) AS row_entropy,
             (entropy_k11_k21 + entropy_k12_k22) AS col_entropy,
             entropy_k11_k12_k21_k22 AS matrix_entropy
        FROM llr_step_two
     ),
  llr
  AS (SELECT indicator_id,
             col_num,
             row_num,
             k11,
             k12,
             k21,
             k22,
             xlog_k11,
             xlog_k12,
             xlog_k21,
             xlog_k22,
             entropy_k11_k12,
             entropy_k21_k22,
             entropy_k11_k21,
             entropy_k12_k22,
             entropy_k11_k12_k21_k22,
             row_entropy,
             col_entropy,
             matrix_entropy,
             ROUND(CASE WHEN ((row_entropy + col_entropy) > matrix_entropy)
                        THEN 0.0
                        ELSE (2.0 * (matrix_entropy - row_entropy - col_entropy))
                    END, 4) AS llr
        FROM llr_step_three
     ),
  top_k
  AS (SELECT indicator_id,
             row_num,
             col_num,
             llr,
             ROW_NUMBER() OVER (PARTITION BY indicator_id, row_num ORDER BY llr DESC) AS rank
        FROM llr
       WHERE llr > 0.0
     )
--select * from primary_user_items;
--select * from secondary_user_items;
--select * from primary_interaction_counts;
--select * from secondary_interaction_counts;
--select * from at_b_sql ORDER BY 1, 2, 3;
--select * from at_b_sql_ut ORDER BY 1, 3, 2;
--select * from at_b_join ORDER BY 1, 2;
INSERT
  INTO cco_cross_similarities
       (target_id, other_target_id, indicator_id, score)
SELECT row_num,
       col_num,
       indicator_id,
       llr
  FROM top_k
 WHERE rank <= (SELECT max_related_count FROM config)
 ORDER BY 3, 1, 4 DESC;

COMMIT;

