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
                person_id int,
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

        // TODO I had to add an IFNULL for the birthdate_estimated, unclear why this wasn't needed before
        // TODO I had to add IFNULL for creator and set to 1 if null, unclear why this wasn't needed before, and is this correct?
        executeMysql("Insert Patients into Person Table",
        '''  
            insert into person (person_id, uuid, gender, birthdate, birthdate_estimated, creator, date_created, dead, death_date, cause_of_death)
                select
                    p.person_id,
                    p.person_uuid as uuid,
                    p.gender,
                    p.birthdate,
                    IFNULL(p.birthdate_estimated,0),
                    IFNULL(u.user_id,1) as creator,
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

        // insert into person_name table
        // TODO I had to add IFNULL for creator and set to 1 if null, unclear why this wasn't needed before, and is this correct?
        executeMysql("Insert Patients into Person Name Table",
        '''
            insert into person_name(person_id, uuid, given_name, family_name, preferred, creator, date_created)
            select
                p.person_id,
                uuid() as uuid,
                concat(p.first_name, if(p.first_name2 is null, '', concat(' ', p.first_name2))) as given_name,
                p.last_name as family_name,
                1 as preferred,
                IFNULL(u.user_id,1) as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            order by p.source_patient_id;
        ''')

        // TODO do we need to actually try to set creator correctly
        // TODO need to figure out how to handle patients with address > 255 characters (add link to ticket)
        executeMysql("Insert Patients into Person Address Table",
        '''
            insert into person_address(person_id, preferred, address1, address2, city_village, state_province, country, creator, date_created, address3, uuid) 
            select  person_id, 
                    case when (address_type = 'current') then 1 else 0 end as 'preferred',
                    locality as address1, 
                    left(address,255) as address2, 
                    commune as city_village, 
                    department as state_province, 'Haiti' as country, 
                    1 as creator, 
                    entry_date as date_created, 
                    section as address3, 
                    uuid() as uuid
            from hivmigration_patient_addresses 
            order by person_id 
        ''')

        // TODO I had to add IFNULL for creator and set to 1 if null, unclear why this wasn't needed before, and is this correct?
        executeMysql("Insert Patients into Patient Table",
        '''
            insert into patient(patient_id, creator, date_created)
            select
                p.person_id as patient_id,
                IFNULL(u.user_id,1) as creator,
                date_format(p.patient_created_date, '%Y-%m-%d %T') as date_created
            from
                hivmigration_patients p
            left join
                hivmigration_users hu on p.patient_created_by = hu.source_user_id
            left join
                users u on u.uuid = hu.user_uuid
            order by p.source_patient_id;
        ''')
    }

    @Override
    def void revert() {

        // remove patients from person, person_name and person_address template
        executeMysql("Remove Patients from Patient, Person Address, Person Name, and Person tables",
        '''
            delete from patient where patient_id in (select person_id from hivmigration_patients);
            delete from person_address where person_id in (select person_id from hivmigration_patient_addresses);
            delete from person_name where person_id in (select person_id from hivmigration_patients);
            delete from person where person_id in (select person_id from hivmigration_patients);
        ''')

        // remove staging tables
        executeMysql("Remove Patient staging tables",
        '''
            drop table if exists hivmigration_zlemrid;
            drop table if exists hivmigration_patient_addresses;
            drop table if exists hivmigration_patients;
        ''')

    }
}
