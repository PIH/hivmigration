package org.pih.hivmigration.etl.sql

class InfantMigrator extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql("Create Infants Staging Table", '''
            create table hivmigration_infants (
                person_id int PRIMARY KEY AUTO_INCREMENT,
                source_infant_id int,
                mother_patient_id int,
                person_uuid char(38),
                infant_code varchar(20),
                first_name varchar(100),
                last_name varchar(100),
                gender varchar(50),
                birthdate date,
                health_center int,
                hiv_status varchar(16),
                vital_status varchar(16),
                vital_status_date date,
                non_emr_mother varchar(48),
                father_patient_id int
            );
        ''')

        setAutoIncrement("hivmigration_infants", "(select (max(person_id)+1) from person)")

        loadFromOracleToMySql('''
                insert into hivmigration_infants (
                    source_infant_id,
                    mother_patient_id,
                    infant_code,
                    first_name,
                    last_name,
                    gender,
                    birthdate,
                    health_center,
                    hiv_status,
                    vital_status,
                    vital_status_date,
                    non_emr_mother,
                    father_patient_id                    
                ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', '''
                select 
                    i.INFANT_ID,
                    i.MOTHER_PATIENT_ID,
                    TRIM(i.INFANT_CODE), 
                    i.FIRST_NAME,
                    i.LAST_NAME, 
                    upper(i.GENDER) as gender, 
                    i.BIRTH_DATE,
                    i.HEALTH_CENTER,                   
                    i.HIV_STATUS,                    
                    i.VITAL_STATUS,
                    to_char(i.VITAL_STATUS_DATE, 'yyyy-mm-dd') as vital_status_date,
                    i.NON_EMR_MOTHER,
                    i.FATHER_PATIENT_ID
                from HIV_INFANTS i 
                where (i.patient_id is null) 
                    and ((i.MOTHER_PATIENT_ID is null) or (
                        i.MOTHER_PATIENT_ID is not null 
                        and i.MOTHER_PATIENT_ID in (select patient_id from HIV_DEMOGRAPHICS_REAL))) 
                    and ((i.FATHER_PATIENT_ID is null) or (
                        i.FATHER_PATIENT_ID is not null 
                        and i.FATHER_PATIENT_ID in (select patient_id from HIV_DEMOGRAPHICS_REAL))); 
        ''')

        loadFromOracleToMySql('''
                insert into hivmigration_data_warnings (                    
                    field_name,
                    field_value,
                    warning_type,
                    warning_details,
                    flag_for_review
                ) values (?, ?, ?, ?, ?)
            ''', '''
                select                     
                    'infant_id',
                    i.INFANT_ID, 
                    'Infant has patient record, skipping importing infant record',
                    'Infant with INFANT_ID ' || i.INFANT_ID || ' has patient record with with PATIENT_ID ' || i.PATIENT_ID || ', skipping importing infant record',
                    1 			
                from HIV_INFANTS i 
                where i.patient_id is not null; 
        ''')

        executeMysql("Add UUIDs", "update hivmigration_infants set person_uuid = uuid();")

        executeMysql("Load to person table", '''

            SET @UNKNOWN_CAUSE_OF_DEATH_ID = (SELECT concept_id FROM concept WHERE uuid='3cd6fac4-26fe-102b-80cb-0017a47871b2');
            
            insert into person( 
                person_id, 
                uuid, 
                gender, 
                birthdate, 
                creator, 
                date_created,
                dead, 
                death_date, 
                cause_of_death)
            select
                i.person_id,
                i.person_uuid,
                COALESCE(i.gender, 'U'),  # most of the data is missing gender -- default to U
                i.birthdate,
                1,
                date_format(curdate(), '%Y-%m-%d %T') as date_created, 
                case when (i.vital_status ='dead') then (1) else 0 end as dead,
                case when (i.vital_status ='dead') then (i.vital_status_date) else null end as death_date,
                case when (i.vital_status ='dead') then @UNKNOWN_CAUSE_OF_DEATH_ID else null end as cause_of_death                 
            from
                hivmigration_infants i 
            order by i.source_infant_id;
        ''')

        executeMysql("Load infant names to person_name table", '''
            insert into person_name (
                person_id, uuid, given_name, family_name, preferred, creator, date_created
            )
            select
              i.person_id,
              uuid(),
              i.first_name,
              i.last_name,
              1,
              1,
              date_format(curdate(), '%Y-%m-%d %T')
            from
              hivmigration_infants i
        ''')

        // log birthdates that are null, in the future, or older than 30 years
        executeMysql("Log abnormal birthdate values", '''
                insert into hivmigration_data_warnings (                    
                    openmrs_patient_id,
                    field_name,
                    field_value,
                    warning_type,                    
                    flag_for_review) 
                select
                	p.person_id as openmrsPatientId,                     
                    'birthdate' as fieldName,
                    p.birthdate as fieldValue,
                    'Abnormal birthdate value' as warningType,
                    1
                from person p 
                where (p.person_id in (select person_id from hivmigration_infants))
                    and (birthdate is null OR birthdate > now() OR birthdate < (SELECT DATE_SUB(now(), INTERVAL 30 YEAR))); 
        ''')

        // TODO: see https://pihemr.atlassian.net/browse/UHM-4817
        executeMysql("Note NULL names", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type, flag_for_review)
            SELECT
                person_id,
                CASE
                    WHEN given_name IS NULL AND family_name IS NULL THEN 'given name and family name'
                    WHEN given_name IS NULL THEN 'given name'
                    WHEN family_name IS NULL THEN 'family name'
                END,
                NULL,
                'Infant missing name. Defaulted to "UNKNOWN"',
                TRUE
            FROM person_name
            WHERE (given_name IS NULL OR family_name IS NULL)
                AND person_id in (select person_id from hivmigration_infants);
        ''')
        executeMysql("Set NULL names to UNKNOWN", '''
            update
                person_name set given_name='UNKNOWN'
            where
                given_name is null and
                person_id in (select person_id from hivmigration_infants);
            
            update
                person_name set family_name='UNKNOWN'
            where
                family_name is null and
                person_id in (select person_id from hivmigration_infants);
        ''')

        executeMysql("Load infants into patient table", '''
            insert into patient (patient_id, creator, date_created)
            select
              i.person_id,
              1,
              date_format(curdate(), '%Y-%m-%d %T')
            from
              hivmigration_infants i
            order by i.source_infant_id;
        ''')

        executeMysql("Create empty registration encounter for each infant", '''
            INSERT INTO encounter(
                encounter_type, 
                patient_id, 
                location_id, 
                form_id, 
                encounter_datetime, 
                creator, 
                date_created, 
                voided, 
                uuid)
            SELECT
                (SELECT encounter_type_id FROM encounter_type WHERE name = 'Enregistrement de patient') as encounter_type,
                person_id,
                IFNULL(hhc.openmrs_id, 1) as location_id,
                (SELECT form_id FROM form WHERE uuid = '6F6E6FA0-1E99-41A3-9391-E5CB8A127C11') as form_id,
                IFNULL(birthdate, now()) as encounter_datetime,
                1,
                now() as date_created,
                0,
                uuid() as uuid
            FROM hivmigration_infants i
            LEFT JOIN hivmigration_health_center hhc ON i.health_center = hhc.hiv_emr_id;
        ''')

        executeMysql("Load ZL IDs", '''
            insert into patient_identifier (
              patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created
            )
            select
              p.person_id as patient_id,
              uuid() as uuid,
              (SELECT patient_identifier_type_id from patient_identifier_type where name = 'ZL EMR ID') as identifier_type,
              (SELECT location_id from location where name = 'Unknown Location') as location_id,
              z.zl_emr_id as identifier,
              1 as preferred,
              1 as creator,
              date_format(now(), '%Y-%m-%d %T') as date_created
            from
              hivmigration_infants p
            join
              hivmigration_zlemrid z on p.person_id = z.person_id
            order by p.source_infant_id;
        ''')

        executeMysql("Create Parent-Child relationship for mothers", '''
            insert into relationship
                (person_a, relationship, person_b, start_date, creator, date_created, uuid)
            select
                p.person_id,
                (select relationship_type_id from relationship_type where a_is_to_b='Parent' and b_is_to_a='Child'),
                i.person_id,
                i.birthdate,
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                uuid()
            from hivmigration_infants i 
            join hivmigration_patients p on i.mother_patient_id = p.source_patient_id;
        ''')

        executeMysql("Create Parent-Child relationship for fathers", '''
            insert into relationship
                (person_a, relationship, person_b, start_date, creator, date_created, uuid)
            select
                p.person_id,
                (select relationship_type_id from relationship_type where a_is_to_b='Parent' and b_is_to_a='Child'),
                i.person_id,
                i.birthdate,
                1,
                date_format(curdate(), '%Y-%m-%d %T'),
                uuid()
            from hivmigration_infants i 
            join hivmigration_patients p on i.father_patient_id = p.source_patient_id;
        ''')

        executeMysql("Insert HIV EMR V1 Infant ID into Patient Identifier Table",
                '''
            insert into patient_identifier(
                patient_id, 
                uuid, 
                identifier_type, 
                location_id, 
                identifier, 
                preferred, 
                creator, 
                date_created)
            select
                i.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'HIV EMR V1 Infant ID') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                i.source_infant_id as identifier,
                0 as preferred,
                1 as creator,
                date_format(curdate(), '%Y-%m-%d %T') as date_created
            from
                hivmigration_infants i
            where i.source_infant_id is not null;
        ''')

        executeMysql("Insert HIV EMR V1 Infant_Code into Patient Identifier Table",
                '''
            insert into patient_identifier(
                patient_id, 
                uuid, 
                identifier_type, 
                location_id, 
                identifier, 
                preferred, 
                creator, 
                date_created)
            select
                i.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'HIV EMR V1 Infant Code') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                i.infant_code as identifier,
                0 as preferred,
                1 as creator,
                date_format(curdate(), '%Y-%m-%d %T') as date_created
            from
                hivmigration_infants i
            where i.infant_code is not null and i.infant_code != '';
        ''')
    }

    @Override
    def void revert() {

        if (tableExists("hivmigration_infants")) {
            executeMysql("Remove infants from Relationship, Patient, Person Name, Person tables", '''
            delete from relationship where person_b in (select person_id from hivmigration_infants);
            delete from patient_identifier where patient_id in (select person_id from hivmigration_infants);
            delete from test_order where order_id in (select order_id from orders where patient_id in (select person_id from hivmigration_infants));
            delete from orders where patient_id in (select person_id from hivmigration_infants);
            delete from encounter_provider where encounter_id in (select encounter_id from encounter where patient_id in (select person_id from hivmigration_infants));
            delete from encounter where patient_id in (select person_id from hivmigration_infants);  
            delete from patient_program where patient_id in (select person_id from hivmigration_infants);
            delete from visit where patient_id in (select person_id from hivmigration_infants);
            delete from patient where patient_id in (select person_id from hivmigration_infants);
            delete from person_name where person_id in (select person_id from hivmigration_infants);
            delete from person_attribute where person_id in (select person_id from hivmigration_infants);
            delete from person where person_id in (select person_id from hivmigration_infants);
        ''');
        }

        setAutoIncrement("relationship", "select (max(relationship_id)+1) from relationship")
        setAutoIncrement("patient_identifier", "select (max(patient_identifier_id)+1) from patient_identifier")
        setAutoIncrement("person_name", "select (max(person_name_id)+1) from person_name")
        setAutoIncrement("person", "select (max(person_id)+1) from person")
        setAutoIncrement("patient", "select (max(patient_id)+1) from patient")

        executeMysql("drop table if exists hivmigration_infants;");
    }
}
