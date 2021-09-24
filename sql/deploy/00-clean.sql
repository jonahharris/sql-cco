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

DROP VIEW cco_fts_entries;
DROP VIEW cco_labeled_cross_similarities;
DROP VIEW cco_labeled_similarities;
DROP VIEW cco_similarities;

DROP TABLE cco_config;
DROP TABLE cco_cross_similarities;
DROP TABLE cco_events;
DROP TABLE cco_materialized_fts_entries;
DROP TABLE cco_triangular_similarities;
DROP TABLE cco_target_kvpairs;
DROP TABLE raw_events;

/* :vi set ts=2 et sw=2: */
COMMIT;
