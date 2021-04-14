package org.pih.hivmigration.etl.sql

class NutritionMigrator extends ObsMigrator {
    @Override
    def void migrate() {
        // the hivmigration_observations table gets created by ObsLoadingMigrator

        create_tmp_obs_table()

        executeMysql("Migrate nutritional aid boolean", '''
            INSERT INTO tmp_obs
                (source_encounter_id, obs_datetime, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   entry_date,
                   concept_uuid_from_mapping('PIH', '2156'),
                   concept_uuid_from_mapping('PIH', 'NUTRITIONAL AID')
            FROM hivmigration_observations
            WHERE observation = 'receiving_nutritional_assistance_p' and value = 't';
        ''')

        executeMysql("Migrate nutritional aid type", '''
            INSERT INTO tmp_obs
                (source_encounter_id, obs_datetime, concept_uuid, value_text)
            SELECT source_encounter_id,
                   entry_date,
                   concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE COMMENT'),
                   CASE GROUP_CONCAT(distinct(observation) ORDER BY observation)
                    WHEN 'receiving_nutritional_assistance_dry_ration'
                        THEN 'A reçu de la ration sèche.'
                    WHEN 'receiving_nutritional_assistance_financial_aid'
                        THEN 'A reçu de l’aide financière.'
                    WHEN 'receiving_nutritional_assistance_dry_ration,receiving_nutritional_assistance_financial_aid'
                        THEN 'A reçu de la ration sèche et de l’aide financière.'
                    END
            FROM hivmigration_observations
            WHERE (observation = 'receiving_nutritional_assistance_dry_ration'
                       OR observation = 'receiving_nutritional_assistance_financial_aid')
              AND value = 't'
            GROUP BY source_encounter_id;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        executeMysql('''
            DELETE FROM obs WHERE concept_id = concept_from_mapping('PIH', '2156');
            DELETE FROM obs WHERE concept_id = concept_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE COMMENT');
        ''')
    }
}
