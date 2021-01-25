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

        executeMysql("Migrate transport assistance", '''
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'ASSISTANCE WITH TRANSPORT'),
                   CASE comments
                    WHEN 'already_receiving' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE ALREADY RECEIVED')
                    WHEN 'recommended' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                    END
            FROM hivmigration_ordered_other
            WHERE ordered = 'tranportation_aid' AND comments IS NOT NULL AND comments != 'no';
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'NUTRITIONAL AID'),
                   CASE comments
                       WHEN 'recommended' THEN concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED')
                       ELSE concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE ALREADY RECEIVED')
                       END
            FROM hivmigration_ordered_other
            WHERE ordered = 'nutritional_aid' AND comments IS NOT NULL AND comments != 'non';
            
            INSERT INTO tmp_obs
                (source_encounter_id, concept_uuid, value_text)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE NON-CODED'),
                   CASE ordered
                    WHEN 'financial_aid' THEN 'Aide financière\'
                    WHEN 'funeral_aid' THEN \'\'
                    WHEN 'house_assistance' THEN 'Aide au logement\'
                    WHEN 'professional_training' THEN \'\'
                    WHEN 'school_aid' THEN 'Aide scolaire\'
                    WHEN 'social_assistance_other' THEN comments
                    END
            FROM hivmigration_ordered_other
            WHERE ordered IN ('financial_aid', 'funeral_aid', 'house_assistance', 'professional_training', 'school_aid', 'social_assistance_other')
              AND comments IS NOT NULL AND comments NOT LIKE 'no%' AND comments NOT LIKE 'aucun';
            
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE ALREADY RECEIVED'),
                   concept_uuid_from_mapping('CIEL', '5622')  -- Other
            FROM hivmigration_ordered_other
            WHERE ordered IN ('financial_aid', 'funeral_aid', 'house_assistance', 'school_aid', 'social_assistance_other');
            
            INSERT INTO tmp_obs
            (source_encounter_id, concept_uuid, value_coded_uuid)
            SELECT source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'SOCIO-ECONOMIC ASSISTANCE RECOMMENDED'),
                   concept_uuid_from_mapping('CIEL', '5622')  -- Other
            FROM hivmigration_ordered_other
            WHERE ordered = 'professional_training';
        ''')

        migrate_tmp_obs()
    }

    @Override
    def void revert() {
        clearTable("obs")
    }
}
