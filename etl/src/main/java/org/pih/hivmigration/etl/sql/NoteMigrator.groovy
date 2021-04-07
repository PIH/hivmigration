package org.pih.hivmigration.etl.sql

class NoteMigrator extends ObsMigrator {
    @Override
    void migrate() {

        executeMysql("Create note encounters", '''
            insert into encounter (encounter_id, uuid, encounter_datetime, date_created, encounter_type, form_id, patient_id, creator, location_id)
            select
                e.encounter_id,
                e.encounter_uuid,
                IF(e.encounter_date IS NULL, e.date_created, e.encounter_date),
                e.date_created,
                encounter_type('Comment'),
                (select form_id from form where uuid = '9f1e1614-6b7f-423d-a28a-f174042524e2'),
                p.person_id,
                COALESCE(hu.user_id, 1),
                COALESCE(e.location_id, 1)
            from hivmigration_encounters e
            join hivmigration_patients p on e.source_patient_id = p.source_patient_id
            join hivmigration_users hu on e.source_creator_id = hu.source_user_id
            where source_encounter_type = 'note'
              and (note_title is not null or comments is not null);
        ''')

        create_tmp_obs_table()

        executeMysql("Add note text", '''
            insert into tmp_obs
            (source_encounter_id, concept_uuid, value_text)
            select source_encounter_id,
                   concept_uuid_from_mapping('PIH', 'CLINICAL IMPRESSION COMMENTS'),
                   CONCAT(note_title, '\\n\\n', comments)
            from hivmigration_encounters
            where source_encounter_type = 'note' and (note_title is not null or comments is not null);
        ''')

        migrate_tmp_obs()
    }

    @Override
    void revert() {
        executeMysql("delete from encounter where encounter_type = encounter_type('Comment');")
        executeMysql("delete from obs where concept_id = concept_from_mapping('PIH', 'CLINICAL IMPRESSION COMMENTS');")
    }
}
