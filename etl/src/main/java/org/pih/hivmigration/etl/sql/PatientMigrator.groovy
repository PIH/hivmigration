package org.pih.hivmigration.etl.sql

class PatientMigrator extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql("Create Patients Staging Table",
        '''
            create table hivmigration_patients (
                person_id int PRIMARY KEY AUTO_INCREMENT,
                source_patient_id int,
                person_uuid char(38),
                pih_id varchar(100),
                nif_id varchar(100),
                national_id varchar(100),
                first_name varchar(100),
                first_name2 varchar(100),
                last_name varchar(100),
                gender varchar(50),
                birthdate date,
                birthdate_estimated tinyint,
                phone_number varchar(100),
                birth_place varchar(255),
                accompagnateur_name varchar(255),
                patient_created_by int,
                patient_created_date timestamp,
                outcome varchar(255),
                outcome_date date,
                KEY `source_patient_id_idx` (`source_patient_id`),
                UNIQUE KEY `person_uuid_idx` (`person_uuid`)
            );
        ''')

        executeMysql("Create Patient Address Staging Table",
        '''
            create table hivmigration_patient_addresses (
                address_id int PRIMARY KEY,
                source_patient_id int,
                address_type NVARCHAR(8),
                entry_date date,
                address NVARCHAR(512),
                department NVARCHAR(100),
                commune NVARCHAR(100),
                section NVARCHAR(100),
                locality NVARCHAR(100)
            );
        ''')

        executeMysql("Create ZL EMR ID Staging Table",
        '''
            create table hivmigration_zlemrid (
                person_id int PRIMARY KEY AUTO_INCREMENT,
                zl_emr_id varchar(6)
            );
        ''')

        // set autoincrements
        setAutoIncrement("hivmigration_patients", "(select (max(person_id)+1) from person)")
        setAutoIncrement("hivmigration_zlemrid", "(select (max(person_id)+1) from person)")

        // load patients into staging table
        loadFromOracleToMySql('''
                    insert into hivmigration_patients(source_patient_id,pih_id, nif_id, national_id, first_name, first_name2, last_name, gender, birthdate,
                        birthdate_estimated, phone_number, birth_place, accompagnateur_name, patient_created_by, patient_created_date, outcome, outcome_date)
                    values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''',
            '''
                select
                    d.PATIENT_ID as SOURCE_PATIENT_ID,
                    d.PIH_ID,
                    d.NIF_ID,
                    d.NATIONAL_ID,
                    d.FIRST_NAME,
                    d.FIRST_NAME2,
                    d.LAST_NAME,
                    upper(d.GENDER) as GENDER,
                    d.BIRTH_DATE AS BIRTHDATE,
                    decode(d.BIRTH_DATE_EXACT_P, 'f', 1, 't', 0, null) as BIRTHDATE_ESTIMATED,
                    d.PHONE_NUMBER,
                    d.BIRTH_PLACE,
                    d.AGENT as ACCOMPAGNATEUR_NAME,
                    d.PATIENT_CREATED_BY,
                    d.PATIENT_CREATED_DATE,
                    d.TREATMENT_STATUS as outcome,
                    d.TREATMENT_STATUS_DATE as outcome_date
                from HIV_DEMOGRAPHICS_REAL d
                order by  d.PATIENT_ID
            ''');

        // load patient addresses into staging table
        // TODO do we need to use nvarchar?
        loadFromOracleToMySql('''
                    insert into hivmigration_patient_addresses(address_id, source_patient_id, address_type, entry_date, address,
                        department, commune, section, locality)
                    values (?,?,?,?,?,?,?,?,?)
            ''',
                '''
                select
                    a.address_id as address_id,
                    a.patient_id as source_patient_id,
                    a.type as address_type,
                    a.entry_date,
                    a.address,
                    nvl(l.department, a.department) as department,
                    nvl(l.commune, a.commune) as commune,
                    nvl(l.section_communale, a.section) as section,
                    nvl(l.locality, a.locality) as locality
                from HIV_DEMOGRAPHICS_REAL d, hiv_addresses a, hiv_locality l
                where d.patient_id = a.patient_id and a.locality_id = l.locality_id(+)
                order by a.patient_id, decode(a.type, 'current', 1, 0) desc, a.entry_date desc;
            ''');

        // add uuids
        executeMysql("Add UUIDS to Patients",
        '''
            update hivmigration_patients set person_uuid = uuid();
        ''')

        // (this seems to be our current convention in OpenMRS and seemed reasonable to me)
        executeMysql("Set Birthdate Estimate to 0 if Birthdate null",
        '''
            update hivmigration_patients set birthdate_estimated = 0 where birthdate is null
        ''')

        executeMysql("Insert Patients into Person Table",
        '''
            insert into person (person_id, uuid, gender, birthdate, birthdate_estimated, creator, date_created, dead, death_date, cause_of_death)
                select
                    p.person_id,
                    p.person_uuid as uuid,
                    p.gender,
                    p.birthdate,
                    p.birthdate_estimated,
                    u.user_id as creator,
                    date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created,
                    case when (p.outcome='died') then (1) else 0 end as dead,
                    case when (p.outcome='died') then (p.outcome_date) else null end as death_date,
                    case when (p.outcome='died') then ( SELECT concept_id FROM concept WHERE uuid='3cd6fac4-26fe-102b-80cb-0017a47871b2') else null end as cause_of_death
                from
                    hivmigration_patients p
                left join
                    hivmigration_users hu on p.patient_created_by = hu.source_user_id
                left join
                    users u on u.uuid = hu.user_uuid
                order by p.source_patient_id;
        ''')

        executeMysql("Insert Patients into Person Name Table",
        '''
            insert into person_name(person_id, uuid, given_name, family_name, preferred, creator, date_created)
            select
                p.person_id,
                uuid() as uuid,
                concat(p.first_name, if(p.first_name2 is null, '', concat(' ', p.first_name2))) as given_name,
                p.last_name as family_name,
                1 as preferred,
                u.user_id as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            order by p.source_patient_id;
        ''')

        // TODO: see https://pihemr.atlassian.net/browse/UHM-4817
        executeMysql("Note NULL names", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type)
            SELECT
                person_id,
                CASE
                    WHEN given_name IS NULL AND family_name IS NULL THEN 'given name and family name'
                    WHEN given_name IS NULL THEN 'given name'
                    WHEN family_name IS NULL THEN 'family name'
                END,
                NULL,
                'Patient missing name. Defaulted to "UNKNOWN"'
            FROM person_name
            WHERE (given_name IS NULL OR family_name IS NULL)
                AND person_id in (select person_id from hivmigration_patients);
        ''')
        executeMysql("Set NULL names to UNKNOWN", '''
            update
                person_name set given_name='UNKNOWN'
            where
                given_name is null and
                person_id in (select person_id from hivmigration_patients);
            
            update
                person_name set family_name='UNKNOWN'
            where
                family_name is null and
                person_id in (select person_id from hivmigration_patients);
        ''')

        // TODO need to figure out how to handle patients with address > 255 characters, see https://pihemr.atlassian.net/browse/UHM-4751
        executeMysql("Insert Patients into Person Address Table",
        '''
            insert into person_address(person_id, preferred, address1, address2, city_village, state_province, country, creator, date_created, address3, uuid)
            select  p.person_id,
                    case when (pa.address_type = 'current') then 1 else 0 end as 'preferred',
                    pa.locality as address1,
                    left(pa.address,255) as address2,
                    pa.commune as city_village,
                    pa.department as state_province, 'Haiti' as country,
                    1 as creator,
                    pa.entry_date as date_created,
                    pa.section as address3,
                    uuid() as uuid
            from hivmigration_patient_addresses pa, hivmigration_patients p
            where pa.source_patient_id = p.source_patient_id
            order by person_id
        ''')
        executeMysql("Note long addresses", '''
            INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type)
            SELECT p.person_id, 'address', pa.address, 'Address too long. Truncated to 255 characters.'
            FROM  hivmigration_patient_addresses pa, hivmigration_patients p
            WHERE pa.source_patient_id = p.source_patient_id AND LENGTH(pa.address) > 255;
        ''')

        executeMysql("Insert Patients into Patient Table",
        '''
            insert into patient(patient_id, creator, date_created)
            select
                p.person_id as patient_id,
                u.user_id as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            order by p.source_patient_id;
        ''')

        // load in our pre-created batch of new ZL Identifiers
        // TODO: replace with "production" identifiers, see: https://pihemr.atlassian.net/jira/software/c/projects/UHM/issues/UHM-4807
        // TODO: when we replace with "production" identifiers, make sure we have enough for patients and infants
        loadFromCSVtoMySql("insert into hivmigration_zlemrid (zl_emr_id) values (?)", "/sql/patient/zl-identifiers.csv")

        executeMysql("Insert ZL EMR IDs into Patient Identifier Table",
        '''
            insert into patient_identifier(patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created)
            select
                p.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'ZL EMR ID') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                z.zl_emr_id as identifier,
                1 as preferred,
                u.user_id as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            join 
                hivmigration_zlemrid z on p.person_id = z.person_id
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
                order by p.source_patient_id;
        ''')

        executeMysql("Insert HIVEMR-V1 into Patient Identifier Table",
                '''
            insert into patient_identifier(patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created)
            select
                p.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'HIVEMR-V1') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                p.source_patient_id as identifier,
                0 as preferred,
                u.user_id as creator,
            date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            order by p.source_patient_id;
        ''')

        executeMysql("Insert Dossier Number into Patient Identifier Table",
                '''
            insert into patient_identifier(patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created)
            select
                p.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'HIV Nimewo Dosye') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                p.pih_id as identifier,
                0 as preferred,
                u.user_id as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            where p.pih_id is not null
            order by p.source_patient_id;
        ''')

        executeMysql("Insert National Identifier into Patient Identifier Table",
                '''
            insert into patient_identifier(patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created)
            select
                p.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'Carte d\\'identification nationale') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                p.national_id as identifier,
                0 as preferred,
                u.user_id as creator,
            date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            where p.national_id is not null
            order by p.source_patient_id;
        ''')

        executeMysql("Log warnings about duplicate National IDs",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type)       
                SELECT person_id, 'national_id', national_id, 'Duplicate National_ID' 
                from hivmigration_patients where national_id in (      
                        select distinct(national_id)
                        from hivmigration_patients 
                        where national_id is not null  
                        group by national_id 
                        having count(*) > 1 );   
        ''')

        executeMysql("Insert Fiscal Numbers into Patient Identifier Table",
                '''
            insert into patient_identifier(patient_id, uuid, identifier_type, location_id, identifier, preferred, creator, date_created)
            select
                p.person_id as patient_id,
                uuid() as uuid,
                (SELECT patient_identifier_type_id from patient_identifier_type where name = 'Numéro d\\'identité fiscale (NIF)') as identifier_type,
                (SELECT location_id from location where name = 'Unknown Location') as location_id,
                p.nif_id as identifier,
                0 as preferred,
                u.user_id as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
                where p.nif_id is not null
                order by p.source_patient_id;
        ''')

        executeMysql("Log warnings about duplicate Fiscal Numbers",
                '''
                INSERT INTO hivmigration_data_warnings (openmrs_patient_id, field_name, field_value, warning_type)       
                SELECT person_id, 'nif_id', nif_id, 'Duplicate nif_id' 
                from hivmigration_patients where nif_id in (      
                        select distinct(nif_id)
                        from hivmigration_patients 
                        where nif_id is not null and nif_id != ' ' 
                        group by nif_id 
                        having count(*) > 1 );   
        ''')

        executeMysql('Insert Phone Numbers into Person Attribute Table',
            '''
            insert into person_attribute(person_id, value, person_attribute_type_id, creator, date_created, uuid)
                select
                    p.person_id as person_id,
                    p.phone_number as value,
                    (SELECT person_attribute_type_id from person_attribute_type where name = 'Telephone Number') as person_attribute_type_id,
                    u.user_id as creator,
                    date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created,
                    uuid() as uuid
                from
                    hivmigration_patients p
                left join
                    hivmigration_users hu on p.patient_created_by = hu.source_user_id
                left join
                    users u on u.uuid = hu.user_uuid
                where p.phone_number is not null
                order by p.source_patient_id;
        ''')
    }


    @Override
    def void revert() {

        if (tableExists("hivmigration_patients")) {
            executeMysql("Remove Patients from Patient, Patient Identifier, Person Name, Person Addresses, Person Attribute and Person tables",
                    '''
            delete from person_attribute where person_id in (select person_id from hivmigration_patients);
            delete from patient_identifier where patient_id in (select person_id from hivmigration_patients);
            delete from patient where patient_id in (select person_id from hivmigration_patients);
            delete from person_address where person_id in (select person_id from hivmigration_patients);
            delete from person_name where person_id in (select person_id from hivmigration_patients);
            delete from person where person_id in (select person_id from hivmigration_patients);
        ''')
            setAutoIncrement("person_attribute", "select (max(person_attribute_id) + 1) from person_attribute")
            setAutoIncrement("person_address", "select (max(person_address_id)+1) from person_address")
            setAutoIncrement("patient_identifier", "select (max(patient_identifier_id)+1) from patient_identifier")
            setAutoIncrement("person_name", "select (max(person_name_id)+1) from person_name")
            setAutoIncrement("person", "select (max(person_id)+1) from person")
            setAutoIncrement("patient", "select (max(patient_id)+1) from patient")
        }

        // remove staging tables
        executeMysql("Remove Patient staging tables",
        '''
            drop table if exists hivmigration_zlemrid;
            drop table if exists hivmigration_patient_addresses;
            drop table if exists hivmigration_patients;
        ''')

    }
}
