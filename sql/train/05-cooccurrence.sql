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
  -- select primary indicator events (A)
  user_items
  AS (SELECT entity_id AS row_num,
             target_id AS col_num
        FROM cco_downsampled_events
       WHERE indicator_id = (SELECT indicator_id
                               FROM config
                               JOIN cco_indicators
                                    ON (indicator = primary_indicator))
       ORDER BY 1, 2
     ),
  interaction_counts
  AS (SELECT col_num,
             COUNT(*) AS interaction_count
        FROM user_items
       GROUP BY 1
     ),
  -- compute the cooccurrence matrix A'A
  item_item_upper_triangular
  AS (SELECT u1.col_num AS col_num,
             u2.col_num AS row_num,
             COUNT(*) as value
        FROM user_items u1
        JOIN user_items u2
             ON (u1.row_num = u2.row_num)
       WHERE u1.col_num < u2.col_num
      GROUP BY u1.col_num, u2.col_num
     ),
  llr_input_ut
  AS (SELECT ii.col_num,
             ii.row_num,
             SUM(ii.value) AS nb_interactions_with_a_b,
             a.interaction_count AS nb_interactions_with_a,
             b.interaction_count AS nb_interactions_with_b,
             (SELECT COUNT(DISTINCT row_num) FROM user_items) AS nb_interactions
        FROM item_item_upper_triangular ii
        JOIN interaction_counts a
             ON (a.col_num = ii.row_num)
        JOIN interaction_counts b
             ON (b.col_num = ii.col_num)
       GROUP BY ii.col_num, ii.row_num
     ),
  llr_base
  AS (SELECT col_num,
             row_num,
             nb_interactions_with_a_b AS k11,
             (nb_interactions_with_a - nb_interactions_with_a_b) AS k12,
             (nb_interactions_with_b - nb_interactions_with_a_b) AS k21,
             (nb_interactions - nb_interactions_with_a - nb_interactions_with_b + nb_interactions_with_a_b) AS k22
        FROM llr_input_ut
     ),
  llr_step_one
  AS (SELECT col_num,
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
  AS (SELECT col_num,
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
  AS (SELECT col_num,
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
  AS (SELECT col_num,
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
/*
  llr_expanded
  AS (SELECT col_num, row_num, llr FROM llr
      UNION ALL
      SELECT row_num, col_num, llr FROM llr
     ),
*/
  top_k
  AS (SELECT col_num,
             row_num,
             llr,
             ROW_NUMBER() OVER (PARTITION BY row_num ORDER BY llr DESC) AS rank
        --FROM llr_expanded
        FROM llr
       WHERE llr > 0.0
     )
--SELECT * FROM llr_input;
--SELECT * FROM llr_input_ut;
/*
SELECT col_num, row_num, llr, rank FROM top_k
UNION ALL
SELECT row_num, col_num, llr, rank FROM top_k
ORDER BY 1, 2, 3
;
.quit
*/
INSERT
  INTO cco_triangular_similarities
       (lt_target_id, gt_target_id, score)
SELECT col_num,
       row_num,
       llr
  FROM top_k
 WHERE rank <= (SELECT max_related_count FROM config);

COMMIT;

