package org.pih.hivmigration.etl.sql

class StagingTablesMigrator extends SqlMigrator {
    @Override
    def void migrate() {
        (new TableStager("HIV_INTAKE_FORMS")).migrate()
        (new TableStager("HIV_FOLLOWUP_FORMS")).migrate()
        (new TableStager("HIV_DIAGNOSES")).migrate()
        (new TableStager("HIV_PATIENT_DIAGNOSES")).migrate()
    }

    @Override
    def void revert() {
        (new TableStager("HIV_PATIENT_DIAGNOSES")).revert()
        (new TableStager("HIV_DIAGNOSES")).revert()
        (new TableStager("HIV_FOLLOWUP_FORMS")).revert()
        (new TableStager("HIV_INTAKE_FORMS")).revert()
    }
}
