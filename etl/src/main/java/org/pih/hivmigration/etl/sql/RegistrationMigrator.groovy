package org.pih.hivmigration.etl.sql

class RegistrationMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create Patient Registration encounter Staging Table",
                '''
            create table hivmigration_registration_encounters (    
                encounter_id int PRIMARY KEY AUTO_INCREMENT,   
                encounter_date date,         
                source_patient_id int,                                
                creator int
            );
        ''')

        executeMysql("Create Patient Birthplace Staging Table",
                '''
            create table hivmigration_patient_birthplace (    
                obs_id int PRIMARY KEY AUTO_INCREMENT,   
                encounter_id int,                         
                source_patient_id int,                                                
                department NVARCHAR(100),
                commune NVARCHAR(100),
                section NVARCHAR(100),
                locality NVARCHAR(100)
            );
        ''')

        setAutoIncrement('hivmigration_registration_encounters', '(select max(encounter_id)+1 from encounter)')
        // load registration encounters into staging table
        executeMysql("Load registration encounters into staging table",'''
                    insert into hivmigration_registration_encounters(
                            source_patient_id, 
                            encounter_date,
                            creator
                        )                    
                    select                    
                        p.source_patient_id,   
                        p.patient_created_date,
                        u.user_id as creator
                    from hivmigration_patients p
                    left join 
                        hivmigration_users hu on p.patient_created_by = hu.source_user_id
                    left join
                        users u on u.uuid = hu.user_uuid;
            ''')

        setAutoIncrement("hivmigration_patient_birthplace", "(select max(obs_id)+1 from obs)")

        // load patients birthplace into staging table
        loadFromOracleToMySql('''
                    insert into hivmigration_patient_birthplace(
                        source_patient_id,                         
                        department, 
                        commune, 
                        section, 
                        locality)
                    values (?,?,?,?,?)
            ''',
                '''
                select                    
                    d.patient_id as source_patient_id,                                       
                    l.department as department,
                    l.commune as commune,
                    l.section_communale as section,
                    nvl(l.locality, d.BIRTH_PLACE) as locality
                from HIV_DEMOGRAPHICS_REAL d left outer join hiv_locality l
                     on d.BIRTH_PLACE_LOCALITY = l.locality_id
                where l.department is not null
                    or l.commune is not null 
                    or l.section_communale is not null 
                    or l.locality is not null 
                    or d.BIRTH_PLACE is not null;
        ''')

        executeMysql("Create Registration encounters",'''

            SET @encounter_type_registration = (select encounter_type_id from encounter_type where uuid = '873f968a-73a8-4f9c-ac78-9f4778b751b6');
            SET @form_registration = (select form_id from form where uuid = '6F6E6FA0-1E99-41A3-9391-E5CB8A127C11');
            
            insert into encounter(
                   encounter_id, 
                   uuid, 
                   encounter_datetime, 
                   date_created, 
                   encounter_type, 
                   form_id, 
                   patient_id, 
                   creator, 
                   location_id) 
            select 
                    r.encounter_id,
                    uuid(),
                    r.encounter_date,
                    r.encounter_date,
                    @encounter_type_registration,
                    @form_registration,
                    p.person_id,
                    r.creator,
                    1    
            from hivmigration_registration_encounters r, hivmigration_patients p
            where r.source_patient_id = p.source_patient_id;
            
            update hivmigration_patient_birthplace b
            join hivmigration_patients hp on b.source_patient_id = hp.source_patient_id
            join encounter e on hp.person_id = e.patient_id and e.encounter_type = @encounter_type_registration
            set b.encounter_id = e.encounter_id;
        ''')

        create_tmp_obs_table()

        executeMysql("Load birthplaces as obs",'''

            ALTER TABLE hivmigration_patient_birthplace ADD COLUMN encounter_date DATETIME;
            
            UPDATE hivmigration_patient_birthplace b
            JOIN hivmigration_registration_encounters e ON b.encounter_id = e.encounter_id
            SET b.encounter_date = e.encounter_date;
            
            -- Create Birthplace address construct
            INSERT INTO tmp_obs (
                obs_id,
                encounter_id,
                obs_datetime,
                concept_uuid)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Birthplace address construct')
            from    hivmigration_patient_birthplace b
            where locality is not null or section is not null or commune is not null or department is not null;
            
            -- Add Locality
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Address1'),
                locality
            from    hivmigration_patient_birthplace
            where   locality is not null;
            
            -- Add Section Communale
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Address3'),
                section
            from    hivmigration_patient_birthplace
            where   section is not null;
            
            
            -- Add Commune
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'City Village'),
                commune
            from    hivmigration_patient_birthplace
            where   commune is not null;
            
            -- Add Department
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'State Province'),
                department
            from    hivmigration_patient_birthplace
            where   department is not null;
            
            -- Add Country
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Country'),
                'Haiti'
            from    hivmigration_patient_birthplace;
        ''')

        migrate_tmp_obs()

        executeMysql("Add civil status obs", '''
            -- Add CIVIL STATUS
            SET @civil_status_concept = (concept_from_mapping('PIH', 'CIVIL STATUS'));
              
            INSERT INTO obs (                    
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_coded,                
                    creator, 
                    date_created, 
                    uuid)
            select 
                    p.person_id, 
                    r.encounter_id, 
                    r.encounter_date as obs_datetime, 
                    1 as locationId, 
                    @civil_status_concept as concept_id,  
                    CASE UPPER(TRIM(s.civil_status)) 
                        WHEN 'DIVORCED' THEN concept_from_mapping('PIH', 'DIVORCED') 
                        WHEN 'MARRIED' THEN concept_from_mapping('PIH', 'MARRIED') 
                        WHEN 'PARTNER' THEN concept_from_mapping('PIH', 'LIVING WITH PARTNER') 
                        WHEN 'PLACE' THEN concept_from_mapping('PIH', 'LIVING WITH PARTNER') 
                        WHEN 'SEPARATED' THEN concept_from_mapping('PIH', 'SEPARATED') 
                        WHEN 'SINGLE' THEN concept_from_mapping('PIH', 'SINGLE OR A CHILD') 
                        WHEN 'WIDOWED' THEN concept_from_mapping('PIH', 'WIDOWED') 
                        ELSE concept_from_mapping('PIH', 'OTHER') 
                    END as value_coded, 
                    r.creator, r.encounter_date as dateCreated, uuid() as uuid
            from hivmigration_socioeconomics s, hivmigration_registration_encounters r, hivmigration_patients p 
            where  s.civil_status is not null and s.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id;                                                                                                                                                                                                     
        ''')

    }

    @Override
    def void revert() {
        if(tableExists("hivmigration_registration_encounters")) {
            executeMysql("delete from obs where (encounter_id in (select encounter_id from hivmigration_registration_encounters)) and (obs_group_id is not null)")
            executeMysql("delete from obs where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("delete from encounter where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("drop table if exists hivmigration_patient_birthplace")
            executeMysql("drop table if exists hivmigration_registration_encounters")
        }
    }
}
