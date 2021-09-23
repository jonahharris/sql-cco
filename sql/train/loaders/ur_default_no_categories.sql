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

DROP TABLE IF EXISTS raw_events;

.import --csv ./data/raw/events-no-categories.csv raw_events

/*
 * During feature analysis, it appears vPaaS "age in years" ranges between
 * [18, 250]. As such, limit this to the categorical ranges we found worked
 * well in RECON V1.
 */
/*
UPDATE raw_events
   SET viewer_age = min(max(18, viewer_age), 65),
       broadcaster_age = min(max(18, broadcaster_age), 65);
*/

INSERT INTO cco_events
            (entity, indicator, target)
     SELECT entity, indicator, target
       FROM raw_events;

COMMIT;

VACUUM;
/* :vi set ts=2 et sw=2: */
