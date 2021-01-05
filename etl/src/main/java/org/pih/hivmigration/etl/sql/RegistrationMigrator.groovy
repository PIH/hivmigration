package org.pih.hivmigration.etl.sql

class RegistrationMigrator extends SqlMigrator {

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

        executeMysql("Create Patient Socio-Economics Staging Table",
                '''
            create table hivmigration_socio_economics (    
                obs_id int PRIMARY KEY AUTO_INCREMENT,                            
                source_patient_id int,                                
                civil_status NVARCHAR(16),
                primary_activity NVARCHAR(96),
                other_sexual_partners text,
                partners_same_town_p char(1) DEFAULT NULL,
                partners_other_town text,
                residences text,                
                nutritional_evaluation text,
                num_people_in_house decimal(10,0) DEFAULT NULL,
                num_rooms_in_house decimal(10,0) DEFAULT NULL,
                type_of_roof text,
                type_of_floor text,
                latrine_p char(1) DEFAULT NULL,
                radio_p char(1) DEFAULT NULL,
                education text,
                other_sexual_partners_p char(1) DEFAULT NULL
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
            ''');

        // load patients CIVIL_STATUS and OCCUPATION into the staging table
        loadFromOracleToMySql('''
                    insert into hivmigration_socio_economics(
                        source_patient_id,                         
                        civil_status, 
                        primary_activity,
                        other_sexual_partners,
                        partners_same_town_p,
                        partners_other_town,
                        residences,                
                        nutritional_evaluation,
                        num_people_in_house,
                        num_rooms_in_house,
                        type_of_roof,
                        type_of_floor,
                        latrine_p,
                        radio_p,
                        education,
                        other_sexual_partners_p)
                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''',
                '''
                    select 
                        s.patient_id as source_patient_id, 
                        UPPER(TRIM(s.civil_status)) as civil_status, 
                        UPPER(TRIM(s.PRIMARY_ACTIVITY)) as primary_activity,
                        s.other_sexual_partners,
                        s.partners_same_town_p,
                        s.partners_other_town,
                        s.residences,                
                        s.nutritional_evaluation,
                        s.num_people_in_house,
                        s.num_rooms_in_house,
                        s.type_of_roof,
                        s.type_of_floor,
                        s.latrine_p,
                        s.radio_p,
                        s.education,
                        s.other_sexual_partners_p 
                    from HIV_SOCIOECONOMICS s, hiv_demographics_real p 
                    where (s.patient_id = p.patient_id) and ((s.civil_status is not null) or (s.PRIMARY_ACTIVITY is not null)) order by s.patient_id;
            ''');

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
        ''')


        executeMysql("Load birthplaces as obs",'''

            -- Create Birthplace address construct
            SET @birthplace_construct = (concept_from_mapping('PIH', 'Birthplace address construct'));
            
            INSERT INTO obs (
                    obs_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,                    
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @birthplace_construct,
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and ( b.locality is not null or b.section is not null or b.commune is not null or b.department is not null); 
                    
            -- Add Locality
            SET @locality_concept = (concept_from_mapping('PIH', 'Address1'));  
            INSERT INTO obs (
                    obs_group_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_text,                
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @locality_concept,
                    b.locality,
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and b.locality is not null;     
                    
            -- Add Section Communale
            SET @section_communale_concept = (concept_from_mapping('PIH', 'Address3'));  
            INSERT INTO obs (
                    obs_group_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_text,                
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @section_communale_concept,
                    b.section,
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and b.section is not null; 
                    
                    
            -- Add Commune
            SET @commune_concept = (concept_from_mapping('PIH', 'City Village'));  
            INSERT INTO obs (
                    obs_group_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_text,                
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @commune_concept,
                    b.commune,
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and b.commune is not null;   
                    
            -- Add Department
            SET @department_concept = (concept_from_mapping('PIH', 'State Province'));  
            INSERT INTO obs (
                    obs_group_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_text,                
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @department_concept,
                    b.department,
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and b.department is not null;  
                    
            -- Add Country
            SET @country_concept = (concept_from_mapping('PIH', 'Country'));  
            INSERT INTO obs (
                    obs_group_id,
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_text,                
                    creator, 
                    date_created, 
                    uuid)
            select   
                    b.obs_id, 
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date,
                    1 as locationId,
                    @country_concept,
                    'Haiti',
                    r.creator,
                    r.encounter_date as dateCreated,
                    uuid() as uuid
            from    hivmigration_patient_birthplace b,  hivmigration_registration_encounters r, hivmigration_patients p 
            where   b.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id 
                    and ( b.locality is not null or b.section is not null or b.commune is not null or b.department is not null);       
                    
                    
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
                    CASE s.civil_status 
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
            from hivmigration_socio_economics s, hivmigration_registration_encounters r, hivmigration_patients p 
            where  s.civil_status is not null and s.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id;                                                                                                                                                                                                     
        ''')

    }

    @Override
    def void revert() {
        if(tableExists("hivmigration_registration_encounters")) {
            executeMysql("delete from obs where (encounter_id in (select encounter_id from hivmigration_registration_encounters)) and (obs_group_id is not null)")
            executeMysql("delete from obs where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("delete from encounter where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("drop table if exists hivmigration_socio_economics")
            executeMysql("drop table if exists hivmigration_patient_birthplace")
            executeMysql("drop table if exists hivmigration_registration_encounters")
        }
    }
}
