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

INSERT INTO cco_entities (entity)
     SELECT DISTINCT entity
       FROM cco_events;

INSERT INTO cco_indicators (indicator)
     SELECT DISTINCT indicator
       FROM cco_events;

INSERT INTO cco_targets (target)
     SELECT DISTINCT target
       FROM cco_events;

/* :vi set ts=2 et sw=2: */
COMMIT;
