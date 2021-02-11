package org.pih.hivmigration.etl.sql

/**
 * Adds fake data of yes on one key population for 20% of patients, within
 * that 20%:
 *
 * +--------------+--------+-----+--------+-----+
 * |              | M < 16 |  M  | F < 16 |  F  |
 * +--------------+--------+-----+--------+-----+
 * | MSM          | 5%     | 50% | 0      | 0   |
 * | sex worker   | 0      | 5%  | 0      | 15% |
 * | prisoner     | 1%     | 10% | 0      | 5%  |
 * | transgender  | 0      | 2%  | 0      | 1%  |
 * | IV drug user | 5%     | 20% | 1%     | 25% |
 * +--------------+--------+-----+--------+-----+
 *
 * Add IV drug user as a second item to 20% Add MSM as a second item to 30%
 *
 * For all non-yeses (ie across all others of these 5 data points across all
 * patients in the system that we have not set to yes via the above rules),
 * randomly have 25% of the answers be unknown and the rest no.
 */
class SampleDataMigrator {


}
