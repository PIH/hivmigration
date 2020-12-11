package org.pih.hivmigration.etl.sql

class StagingTablesMigrator extends SqlMigrator {
    @Override
    def void migrate() {
        (new TableStager("HIV_TB_STATUS")).migrate()
        (new TableStager("HIV_INTAKE_FORMS")).migrate()
        (new TableStager("HIV_FOLLOWUP_FORMS")).migrate()
        (new TableStager("HIV_DIAGNOSES")).migrate()
        (new TableStager("HIV_PATIENT_DIAGNOSES")).migrate()
        (new TableStager("HIV_INTAKE_EXTRA")).migrate()
        (new TableStager("HIV_PREVIOUS_EXPOSURES")).migrate()
        (new TableStager("HIV_DATA_AUDIT")).migrate()
        (new TableStager("HIV_DATA_AUDIT_ENTRY")).migrate()
        (new TableStager("HIV_DATA_AUDIT_TRACKING_FORMS")).migrate()
        (new TableStager("HIV_PATIENT_OBS")).migrate()
        (new TableStager("HIV_COURSE_OF_TX")).migrate()
    }

    @Override
    def void revert() {
        (new TableStager("HIV_COURSE_OF_TX")).revert()
        (new TableStager("HIV_PATIENT_OBS")).revert()
        (new TableStager("HIV_DATA_AUDIT_TRACKING_FORMS")).revert()
        (new TableStager("HIV_DATA_AUDIT_ENTRY")).revert()
        (new TableStager("HIV_DATA_AUDIT")).revert()
        (new TableStager("HIV_INTAKE_EXTRA")).revert()
        (new TableStager("HIV_PREVIOUS_EXPOSURES")).revert()
        (new TableStager("HIV_PATIENT_DIAGNOSES")).revert()
        (new TableStager("HIV_DIAGNOSES")).revert()
        (new TableStager("HIV_FOLLOWUP_FORMS")).revert()
        (new TableStager("HIV_INTAKE_FORMS")).revert()
        (new TableStager("HIV_TB_STATUS")).revert()
    }
}
