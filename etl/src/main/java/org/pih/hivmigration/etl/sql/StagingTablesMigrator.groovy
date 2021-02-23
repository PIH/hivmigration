package org.pih.hivmigration.etl.sql

class StagingTablesMigrator extends SqlMigrator {
    @Override
    def void migrate() {
        migrateTable("HIV_TB_STATUS")
        migrateTable("HIV_INTAKE_FORMS")
        migrateTable("HIV_FOLLOWUP_FORMS")
        migrateTable("HIV_DIAGNOSES")
        migrateTable("HIV_PATIENT_DIAGNOSES")
        migrateTable("HIV_INTAKE_EXTRA")
        migrateTable("HIV_PREVIOUS_EXPOSURES")
        migrateTable("HIV_DATA_AUDIT")
        migrateTable("HIV_DATA_AUDIT_ENTRY")
        migrateTable("HIV_DATA_AUDIT_TRACKING_FORMS")
        migrateTable("HIV_PATIENT_OBS")
        migrateTable("HIV_COURSE_OF_TX")
        migrateTable("HIV_SOCIAL_SUPPORT")
        migrateTable("HIV_SOCIOECONOMICS")
        migrateTable("HIV_SOCIOECONOMICS_EXTRA")
        migrateTable("HIV_DEMOGRAPHICS_AUD")
        migrateTable("HIV_CONTACTS")
        migrateTable("HIV_ORDERED_OTHER")

        executeMysql("Add index to hivmigration_ordered_other.source_encounter_id", '''
            ALTER TABLE hivmigration_ordered_other
            MODIFY source_encounter_id INT;
            
            CREATE INDEX source_encounter_id_idx
            ON hivmigration_ordered_other (`source_encounter_id`);
        ''')
    }
    
    def void migrateTable(String tableName) {
        SqlMigrator migrator = new TableStager(tableName)
        migrator.setRowLimit(rowLimit)
        migrator.migrate()
    }

    @Override
    def void revert() {
        revertTable("HIV_ORDERED_OTHER")
        revertTable("HIV_CONTACTS")
        revertTable("HIV_DEMOGRAPHICS_AUD")
        revertTable("HIV_SOCIOECONOMICS_EXTRA")
        revertTable("HIV_SOCIOECONOMICS")
        revertTable("HIV_SOCIAL_SUPPORT")
        revertTable("HIV_COURSE_OF_TX")
        revertTable("HIV_PATIENT_OBS")
        revertTable("HIV_DATA_AUDIT_TRACKING_FORMS")
        revertTable("HIV_DATA_AUDIT_ENTRY")
        revertTable("HIV_DATA_AUDIT")
        revertTable("HIV_INTAKE_EXTRA")
        revertTable("HIV_PREVIOUS_EXPOSURES")
        revertTable("HIV_PATIENT_DIAGNOSES")
        revertTable("HIV_DIAGNOSES")
        revertTable("HIV_FOLLOWUP_FORMS")
        revertTable("HIV_INTAKE_FORMS")
        revertTable("HIV_TB_STATUS")
    }
    
    def static void revertTable(String tableName) {
        (new TableStager(tableName)).revert()
    }
}
