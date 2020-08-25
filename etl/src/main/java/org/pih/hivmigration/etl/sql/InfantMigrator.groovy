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
                    first_name, last_name,
                    gender,
                    birthdate
                ) values (?, ?, ?, ?, ?, ?, ?)
            ''', '''
                select 
                    i.INFANT_ID,
                    i.MOTHER_PATIENT_ID,
                    i.INFANT_CODE, 
                    i.FIRST_NAME, i.LAST_NAME, 
                    upper(i.GENDER), 
                    i.BIRTH_DATE,  			
                from HIV_INFANTS i
                order by i.INFANT_ID;
        ''')

        executeMysql("Add UUIDS",
                "update hivmigration_infants set person_uuid = uuid();")

        executeMysql("Load to person table", '''
            insert into person
              (person_id, uuid, gender, birthdate, date_created)
            select
              i.person_id,
              i.person_uuid,
              i.gender,
              i.birthdate,
              date_format(curdate(), '%Y-%m-%d %T')
            from
              hivmigration_infants i 
            order by i.source_infant_id;
        ''')
    }

    @Override
    def void revert() {
        executeMysql("Remove infants from Patient, Person Address, Person Name, and Person tables", '''
            delete from patient where patient_id in (select person_id from hivmigration_infants);
            delete from person_name where person_id in (select person_id from hivmigration_infants);
            delete from person where person_id in (select person_id from hivmigration_infants);
        ''');
        executeMysql("drop table if exists hivmigration_infants;");
    }
}
