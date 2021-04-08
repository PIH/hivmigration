package org.pih.hivmigration.etl.sql

/**
 * Regimen Comments are entered on the regimen page, but are not directly associated with any encounters.  These
 * are simply notes made by users, and record who made the comment, when it was made, and the comment itself.
 * For migration, it makes sense to associate these with drug order documentation encounters.
 */
class RegimenCommentMigrator extends SqlMigrator {

    SqlMigrator regimenCommentTableStager = new TableStager("HIV_REGIME_COMMENTS");

    @Override
    void migrate() {
        regimenCommentTableStager.migrate();

        executeMysql("Add and populate column for derived patient id and derived user id", '''

            alter table hivmigration_regime_comments add target_patient_id int;
            alter table hivmigration_regime_comments add target_user_id int;

            update      hivmigration_regime_comments c
            inner join  hivmigration_patients p on c.source_patient_id = p.source_patient_id
            left join   hivmigration_users u on c.entered_by = u.source_user_id
            set         c.target_patient_id = p.person_id,
                        c.target_user_id = ifnull(u.user_id, 1)
            ;
        ''')

        executeMysql("Add and populate column for a derived encounter id", '''
            alter table hivmigration_regime_comments add inferred_encounter_id int;
            
            SET @encounter_type_id = (select encounter_type_id from encounter_type where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002');
            
            update      hivmigration_regime_comments c
            inner join  encounter e on c.target_patient_id = e.patient_id and date(c.entered_date) = date(e.encounter_datetime) and e.encounter_type = @encounter_type_id
            set         c.inferred_encounter_id = e.encounter_id
            ;
        ''')

        executeMysql("Create new encounters for those comments that cannot be linked to one", '''
            
            SET @encounter_type_id = (select encounter_type_id from encounter_type where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002');
            SET @form_id = (select form_id from form where uuid = '96482a6e-5b62-11eb-8f5a-0242ac110002');
            
            insert into encounter (
                    uuid, patient_id, encounter_datetime, encounter_type, form_id, location_id, date_created, creator
            )
            select      uuid(), n.target_patient_id, n.entered_date, @encounter_type_id, @form_id, 1, n.entered_date, 1
            from        (
                        select      c.target_patient_id, date(c.entered_date) as entered_date
                        from        hivmigration_regime_comments c
                        where       c.inferred_encounter_id is null
                        and         c.target_patient_id is not null
                        group by    c.target_patient_id, date(c.entered_date)
            ) n
            ;
        ''')

        executeMysql("Add and populate column for new encounter id", '''

            alter table hivmigration_regime_comments add new_encounter_id int;
            
            SET @encounter_type_id = (select encounter_type_id from encounter_type where uuid = '0b242b71-5b60-11eb-8f5a-0242ac110002');
            
            update      hivmigration_regime_comments c
            inner join  encounter e 
            on          c.target_patient_id = e.patient_id and date(c.entered_date) = date(e.encounter_datetime) and e.encounter_type = @encounter_type_id
            set         c.new_encounter_id = e.encounter_id
            where       c.inferred_encounter_id is null
            ;
        ''')

        executeMysql("Load regimen comments as obs", ''' 

            SET @comments_question = concept_from_mapping('PIH', '10637'); -- Medication comments (text)
                        
            INSERT INTO obs (
                    uuid, person_id, encounter_id, obs_datetime, location_id, concept_id, value_text, creator, date_created
            )
            SELECT  uuid(), c.target_patient_id, ifnull(c.inferred_encounter_id, c.new_encounter_id), date(c.entered_date), 1, 
                    @comments_question, c.comments, c.target_user_id, c.entered_date
            FROM    hivmigration_regime_comments c
            where   c.target_patient_id is not null
            and     ifnull(c.inferred_encounter_id, c.new_encounter_id) is not null
            ;
        ''')

        validateResults()
    }

    @Override
    void revert() {
        if (columnExists("hivmigration_regime_comments", "new_encounter_id")) {
            executeMysql("Deleting obs", '''
                SET @comments_question = concept_from_mapping('PIH', '10637'); -- Medication comments (text)
                
                delete      o.* 
                from        obs o
                inner join  hivmigration_regime_comments c on o.encounter_id = ifnull(c.inferred_encounter_id, c.new_encounter_id)
                where       o.concept_id = @comments_question
                ;
            ''')

            executeMysql("Deleting newly created encounters", '''
                delete      e.* 
                from        encounter e
                inner join  hivmigration_regime_comments c on e.encounter_id = c.new_encounter_id
                ;
            ''')
        }
        regimenCommentTableStager.revert()
    }

    void validateResults() {

        assertMatch(
                "There should be an Obs for each comment",
                "select count(*) as num from hiv_regime_comments c, hiv_demographics_real d where c.patient_id = d.patient_id",
                "select count(*) as num from obs where concept_id = concept_from_mapping('PIH', '10637')"
        )

        assertNoRows(
                "All obs should be associated with an encounter",
                "select count(*) as num from obs where concept_id = concept_from_mapping('PIH', '10637') and encounter_id is null"
        )

        assertNoRows(
                "All obs should have obs value_text",
                "select count(*) as num from obs where concept_id = concept_from_mapping('PIH', '10637') and (value_text is null or value_text = '')"
        )
    }
}
