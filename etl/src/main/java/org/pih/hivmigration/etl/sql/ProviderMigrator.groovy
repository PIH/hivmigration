package org.pih.hivmigration.etl.sql

class ProviderMigrator extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql("Create Providers Staging Table", '''
           create table hivmigration_providers (
                id int PRIMARY KEY AUTO_INCREMENT,
                hiv_provider_name varchar(256)
            );
        ''')

        // load providers
        loadFromOracleToMySql('''
                    insert into hivmigration_providers(hiv_provider_name)
                    values (?)
            ''',
                '''
                select 
                    distinct(UPPER(TRIM(n.name))) as hiv_provider_name
                from 
                    (select performed_by as name from HIV_ENCOUNTERS UNION
                    select EXAMINING_DOCTOR as name from HIV_INTAKE_FORMS UNION
                    select  EXAMINING_DOCTOR as name from HIV_FOLLOWUP_FORMS) n 
                order by UPPER(TRIM(n.name))
            ''')
    }

    @Override
    def void revert() {
        executeMysql('''
            drop table if exists hivmigration_providers;
        ''')
    }
}
