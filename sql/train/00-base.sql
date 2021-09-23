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

DROP TABLE IF EXISTS cco_config;
CREATE TABLE cco_config (
  -- How many related items should be retained per item?
  max_related_count             BIGINT NOT NULL,

  -- How many interactions to (per user) retain after downsampling?
  max_interaction_count         BIGINT NOT NULL,

  -- What's the primary event we want to recommend for?
  primary_indicator             TEXT NOT NULL,

  FOREIGN KEY (primary_indicator) REFERENCES cco_indicators (indicator)
);

-- Match Mahout Default
--INSERT INTO cco_config VALUES (100, 500, 'purchase');

-- UR Default
--INSERT INTO cco_config VALUES (50, 500, 'purchase');

-- ========================================================================= --
-- == SUPPLEMENTARY TABLES/VIEWS =========================================== --
-- ========================================================================= --

DROP TABLE IF EXISTS cco_events;
CREATE TABLE cco_events (
  entity                        TEXT NOT NULL,
  indicator                     TEXT NOT NULL,
  target                        TEXT NOT NULL,
  PRIMARY KEY (entity, indicator, target));

DROP TABLE IF EXISTS cco_entities;
CREATE TABLE cco_entities (
  entity_id                     INTEGER NOT NULL PRIMARY KEY,
  entity                        TEXT NOT NULL UNIQUE);

DROP TABLE IF EXISTS cco_indicators;
CREATE TABLE cco_indicators (
  indicator_id                  INTEGER NOT NULL PRIMARY KEY,
  indicator                     TEXT NOT NULL UNIQUE);

DROP TABLE IF EXISTS cco_targets;
CREATE TABLE cco_targets (
  target_id                     INTEGER NOT NULL PRIMARY KEY,
  target                        TEXT NOT NULL UNIQUE);

DROP TABLE IF EXISTS cco_downsampled_events;
CREATE TABLE cco_downsampled_events (
  entity_id                     INTEGER NOT NULL,
  indicator_id                  INTEGER NOT NULL,
  target_id                     INTEGER NOT NULL,
  FOREIGN KEY (entity_id) REFERENCES cco_entities (entity_id),
  FOREIGN KEY (indicator_id) REFERENCES cco_indicators (indicator_id),
  FOREIGN KEY (target_id) REFERENCES cco_targets (target_id),
  PRIMARY KEY (entity_id, indicator_id, target_id));

DROP TABLE IF EXISTS cco_triangular_similarities;
CREATE TABLE cco_triangular_similarities (
  lt_target_id                  INTEGER NOT NULL,
  gt_target_id                  INTEGER NOT NULL,
  score                         DOUBLE PRECISION NOT NULL,
  FOREIGN KEY (lt_target_id) REFERENCES cco_targets (target_id),
  FOREIGN KEY (gt_target_id) REFERENCES cco_targets (target_id),
  PRIMARY KEY (lt_target_id, gt_target_id));

DROP VIEW IF EXISTS cco_similarities;
CREATE VIEW cco_similarities 
         AS SELECT lt_target_id AS target_id,
                   gt_target_id AS other_target_id,
                   score AS score
              FROM cco_triangular_similarities
             UNION ALL
            SELECT gt_target_id AS target_id,
                   lt_target_id AS other_target_id,
                   score AS score
              FROM cco_triangular_similarities;

DROP VIEW IF EXISTS cco_labeled_similarities;
CREATE VIEW cco_labeled_similarities
         AS SELECT tgt1.target AS target,
                   tgt2.target AS other_target,
                   cs.score AS score
              FROM cco_similarities cs
              JOIN cco_targets tgt1
                   ON (tgt1.target_id = cs.target_id)
              JOIN cco_targets tgt2
                   ON (tgt2.target_id = cs.other_target_id);

DROP TABLE IF EXISTS cco_cross_similarities;
CREATE TABLE cco_cross_similarities (
  target_id                     INTEGER NOT NULL,
  other_target_id               INTEGER NOT NULL,
  indicator_id                  INTEGER NOT NULL,
  score                         DOUBLE PRECISION NOT NULL,
  FOREIGN KEY (target_id) REFERENCES cco_targets (target_id),
  FOREIGN KEY (other_target_id) REFERENCES cco_targets (target_id),
  PRIMARY KEY (target_id, other_target_id, indicator_id));

DROP VIEW IF EXISTS cco_labeled_cross_similarities;
CREATE VIEW cco_labeled_cross_similarities
         AS SELECT tgt1.target AS target,
                   tgt2.target AS other_target,
                   ind.indicator AS indicator,
                   ccs.score AS score
              FROM cco_cross_similarities ccs
              JOIN cco_targets tgt1
                   ON (tgt1.target_id = ccs.target_id)
              JOIN cco_targets tgt2
                   ON (tgt2.target_id = ccs.other_target_id)
              JOIN cco_indicators ind
                   ON (ind.indicator_id = ccs.indicator_id);

DROP VIEW IF EXISTS cco_fts_entries;
CREATE VIEW cco_fts_entries
         AS WITH
            config
            AS (SELECT max_related_count,
                       max_interaction_count,
                       primary_indicator
                  FROM cco_config
                 LIMIT 1
               ),
            indicators
            AS (SELECT cls.target,
                       (SELECT primary_indicator FROM config) AS indicator,
                       cls.score,
                       json_group_array(cls.other_target) AS json_obj
                  FROM cco_labeled_similarities cls
                 GROUP BY 1, 2
                 UNION ALL
                SELECT clcs.target,
                       clcs.indicator,
                       clcs.score,
                       json_group_array(clcs.other_target) AS json_obj
                  FROM cco_labeled_cross_similarities clcs
                 GROUP BY 1, 2
                 ORDER BY indicator, score DESC
               )
          SELECT target,
                 json_group_object(indicator, json(json_obj)) AS cco_json
            FROM indicators
           GROUP BY target
           ORDER BY 1, indicator;

/* :vi set ts=2 et sw=2: */
COMMIT;
