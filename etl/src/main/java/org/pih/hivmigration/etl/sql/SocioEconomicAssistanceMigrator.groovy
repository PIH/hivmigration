package org.pih.hivmigration.etl.sql

/**
 * Migrates the Socio-economic Assistance section of the HIV EMR intake and follow-up
 * forms into the corresponding section of the OpenMRS HIV intake and follow-up forms.
 *
 * Data comes from HIV_ORDERED_OTHER and goes into obs.
 *
 * hivmigration_ordered_other is migrated by StagingTablesMigrator.
 */
class SocioEconomicAssistanceMigrator extends ObsMigrator {

    @Override
    def void migrate() {
        create_tmp_obs_table()

        executeMysql("Migrate transporation aid", '''
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT hoo.source_encounter_id,
                   CASE comments
                       WHEN 'already_receiving' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE ALREADY RECEIVED')
                       WHEN 'recommended' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                       ELSE concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                       END,
                   concept_uuid_from_mapping('PIH', 'ASSISTANCE WITH TRANSPORT')
            FROM hivmigration_ordered_other hoo
            LEFT JOIN hivmigration_intake_forms hif on hoo.source_encounter_id = hif.source_encounter_id
            LEFT JOIN hivmigration_followup_forms hff on hoo.source_encounter_id = hff.source_encounter_id
            WHERE ordered = 'tranportation_aid' AND (hif.form_version != 3 OR hff.form_version != 3 OR comments != 'no');  -- `!= anything` implies IS NOT NULL
        ''')

        executeMysql("Migrate nutritional aid", '''
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT hoo.source_encounter_id,
                   CASE comments
                       WHEN 'already_receiving' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE ALREADY RECEIVED')
                       WHEN 'recommended' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                       ELSE concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                       END,
                   concept_uuid_from_mapping('PIH', 'NUTRITIONAL AID')
            FROM hivmigration_ordered_other hoo
            LEFT JOIN hivmigration_intake_forms hif on hoo.source_encounter_id = hif.source_encounter_id
            LEFT JOIN hivmigration_followup_forms hff on hoo.source_encounter_id = hff.source_encounter_id
            WHERE ordered = 'nutritional_aid' AND (hif.form_version != 3 OR hff.form_version != 3 OR comments != 'no');  -- `!= anything` implies IS NOT NULL
        ''')

        executeMysql("Migrate other aid", '''
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_text)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE NON-CODED'),
                   GROUP_CONCAT(value_text SEPARATOR ', ')
            FROM (
                     SELECT hoo.source_encounter_id,
                            CONCAT(
                                    CASE ordered
                                        WHEN 'financial_aid' THEN 'Aide financière\'
                                        WHEN 'funeral_aid' THEN 'Aide pour funérailles\'
                                        WHEN 'house_assistance' THEN 'Aide au logement\'
                                        WHEN 'professional_training' THEN 'Formation professionnelle\'
                                        WHEN 'school_aid' THEN 'Aide scolaire\'
                                        WHEN 'social_assistance_other' THEN comments
                                        END,
                                    IF(ordered = 'social_assistance_other' OR comments IS NULL, '',
                                       CONCAT(' (',
                                              CASE comments
                                                  WHEN 'recommended' THEN 'Recommandé\'
                                                  WHEN 'already_receiving' THEN 'Reçu\'
                                                  ELSE comments
                                                  END,
                                              ')')
                                        )
                                ) AS value_text
                     FROM hivmigration_ordered_other hoo
                              LEFT JOIN hivmigration_intake_forms hif on hoo.source_encounter_id = hif.source_encounter_id
                              LEFT JOIN hivmigration_followup_forms hff on hoo.source_encounter_id = hff.source_encounter_id
                     WHERE ordered IN ('financial_aid', 'funeral_aid', 'house_assistance', 'professional_training', 'school_aid', 'social_assistance_other')
                         AND hif.form_version != 3 OR hff.form_version != 3 OR (comments NOT LIKE 'no%' AND comments NOT LIKE 'aucun')  -- NOT LIKE implies IS NOT NULL
                ) o
            GROUP BY source_encounter_id
            HAVING max(value_text) IS NOT NULL;
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        clearTable("obs")
    }
}
