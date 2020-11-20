package org.pih.hivmigration.etl.sql

class StagingTablesMigrator extends SqlMigrator {
    @Override
    def void migrate() {
        (new TableStager("HIV_INTAKE_FORMS")).migrate()
    }

    @Override
    def void revert() {
        (new TableStager("HIV_INTAKE_FORMS")).revert()
    }
}
