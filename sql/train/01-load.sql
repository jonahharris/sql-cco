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
INSERT INTO cco_config VALUES (50, 500, 'purchase');

-- ========================================================================= --
-- == DATA-SPECIFIC LOADING ================================================ --
-- ========================================================================= --

DROP TABLE IF EXISTS raw_events;

.import --csv ./data/raw/events.csv raw_events

INSERT INTO cco_events
            (entity, indicator, target)
     SELECT entity, indicator, target
       FROM raw_events;

/* :vi set ts=2 et sw=2: */
COMMIT;
