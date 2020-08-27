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
                birthdate date
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
                    birthdate
                ) values (?, ?, ?, ?, ?, ?, ?)
            ''', '''
                select 
                    i.INFANT_ID,
                    i.MOTHER_PATIENT_ID,
                    i.INFANT_CODE, 
                    i.FIRST_NAME,
                    i.LAST_NAME, 
                    upper(i.GENDER), 
                    i.BIRTH_DATE  			
                from HIV_INFANTS i
        ''')

        executeMysql("Add UUIDs", "update hivmigration_infants set person_uuid = uuid();")

        executeMysql("Load to person table", '''
            insert into person
              (person_id, uuid, gender, birthdate, creator, date_created)
            select
              i.person_id,
              i.person_uuid,
              COALESCE(i.gender, 'U'),  # most of the data is missing gender -- default to U
              i.birthdate,
              1,
              date_format(curdate(), '%Y-%m-%d %T')
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

        executeMysql("Create Mother-Child relationship", '''
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

    }

    @Override
    def void revert() {

        if (tableExists("hivmigration_infants")) {
            executeMysql("Remove infants from Relationship, Patient, Person Name, Person tables", '''
            delete from relationship where person_b in (select person_id from hivmigration_infants);
            delete from patient_identifier where patient_id in (select person_id from hivmigration_infants);
            delete from patient where patient_id in (select person_id from hivmigration_infants);
            delete from person_name where person_id in (select person_id from hivmigration_infants);
            delete from person where person_id in (select person_id from hivmigration_infants);
        ''');
        }

        executeMysql("drop table if exists hivmigration_infants;");
    }
}
