package org.pih.hivmigration.etl.sql

class PcrTestsMigrator extends SqlMigrator {

    @Override
    def void migrate() {

        executeMysql("Create staging table for migrating HIV PCR TEST", '''
            create table hivmigration_pcr_tests (                            
              encounter_id int PRIMARY KEY AUTO_INCREMENT,
              source_encounter_date date,
              source_infant_id int,
              sample_id varchar(255),
              pcr_test_id int,
              result varchar(32), 
              date_of_result date,   
              result_entry_date date,           
              comments varchar(255)            
            );
        ''')

        setAutoIncrement('hivmigration_pcr_tests', '(select max(encounter_id)+1 from encounter)')

        loadFromOracleToMySql('''
            INSERT INTO hivmigration_pcr_tests (                
                source_infant_id,
                sample_id,
                pcr_test_id,
                source_encounter_date,
                result, 
                date_of_result,
                result_entry_date,
                comments
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
                '''
            select  p.INFANT_ID, 
                    p.SAMPLE_ID, 
                    p.PCR_TEST_ID,
                    case 
                        when (p.SAMPLE_DATE is not null ) then to_char(p.SAMPLE_DATE, 'yyyy-mm-dd') 
                        else to_char(p.RESULT_ENTRY_DATE, 'yyyy-mm-dd')  
                    end as encounter_date,         
                    lower(p.RESULT) as result,  
                    to_char(t4.date_result_received, 'yyyy-mm-dd') as date_of_result,                         
                    to_char(p.RESULT_ENTRY_DATE, 'yyyy-mm-dd') as result_entry_date,
                    p.COMMENTS
            from        hiv_pcr_tests p
                left join   (select k.pcr_test_id, min(t.event_date) as date_sent_to_central_site from hiv_lab_tracking t, hiv_pcr_tracking k where t.lab_tracking_id = k.lab_tracking_id and t.event_type = 'sent_to_central_site' group by k.pcr_test_id) t1 on t1.pcr_test_id = p.pcr_test_id
                left join   (select k.pcr_test_id, min(t.event_date) as date_received_at_central_site from hiv_lab_tracking t, hiv_pcr_tracking k where t.lab_tracking_id = k.lab_tracking_id and t.event_type = 'received_at_central_site' group by k.pcr_test_id) t2 on t2.pcr_test_id = p.pcr_test_id
                left join   (select k.pcr_test_id, min(t.event_date) as date_sent_to_lab from hiv_lab_tracking t, hiv_pcr_tracking k where t.lab_tracking_id = k.lab_tracking_id and t.event_type = 'sent_to_lab' group by k.pcr_test_id) t3 on t3.pcr_test_id = p.pcr_test_id
                left join   (select k.pcr_test_id, min(t.event_date) as date_result_received from hiv_lab_tracking t, hiv_pcr_tracking k where t.lab_tracking_id = k.lab_tracking_id and t.event_type = 'result_received' group by k.pcr_test_id) t4 on t4.pcr_test_id = p.pcr_test_id;
        ''')


        executeMysql("Create Lab Results encounters",'''

            SET @encounter_type_lab_results = (select encounter_type_id from encounter_type where uuid = '4d77916a-0620-11e5-a6c0-1697f925ec7b');
            SET @form_lab_results = (select form_id from form where uuid = '4d778ef4-0620-11e5-a6c0-1697f925ec7b');
            
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
                    r.source_encounter_date,
                    now(),
                    @encounter_type_lab_results,
                    @form_lab_results,
                    i.person_id,
                    1,
                    1    
            from hivmigration_pcr_tests r, hivmigration_infants i 
            where r.source_infant_id = i.source_infant_id and r.result is not null;                                                                                      
        ''')

        executeMysql("Create PCR results obs",'''

            -- Add Date of Test Results
            SET @date_of_test_results = (concept_from_mapping('PIH', 'DATE OF LABORATORY TEST'));
            
            INSERT INTO obs (
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,
                    value_datetime, 
                    creator, 
                    date_created, 
                    uuid)
            select 
                    i.person_id, 
                    r.encounter_id, 
                    r.source_encounter_date as obsDatetime, 
                    1 as locationId, 
                    @date_of_test_results as conceptId,
                    r.date_of_result as valueDatetime, 
                    1 as creator, 
                    r.result_entry_date as dateCreated, 
                    uuid() as uuid
            from hivmigration_pcr_tests r, hivmigration_infants i 
            where (r.date_of_result is not null) and (r.source_infant_id = i.source_infant_id) and r.result is not null;  
            
            -- Add PCR Results
            SET @pcr_test_results = (concept_from_mapping('CIEL', '1030'));
            
            INSERT INTO obs (
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,
                    value_coded, 
                    creator, 
                    date_created, 
                    accession_number,
                    comments,
                    uuid)
            select 
                    i.person_id, 
                    r.encounter_id, 
                    r.source_encounter_date as obsDatetime, 
                    1 as locationId, 
                    @pcr_test_results as conceptId,
                    case r.result 
                        when 'negative' then  concept_from_mapping('CIEL', '664') 
                        when 'positive'  then  concept_from_mapping('CIEL', '703') 
                        when 'indeterminate'  then  concept_from_mapping('CIEL', '1138') 
                    end as valueCoded, 
                    1 as creator, 
                    r.result_entry_date as dateCreated, 
                    r.sample_id,
                    r.comments,
                    uuid() as uuid
            from hivmigration_pcr_tests r, hivmigration_infants i 
            where r.source_infant_id = i.source_infant_id and r.result is not null;
            
            -- Add sample ID obs
            INSERT INTO obs (
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
                    i.person_id, 
                    r.encounter_id, 
                    r.source_encounter_date as obsDatetime, 
                    1 as locationId, 
                    concept_from_mapping('CIEL', '162086'),
                    r.sample_id,
                    1 as creator, 
                    r.result_entry_date as dateCreated, 
                    uuid() as uuid
            from hivmigration_pcr_tests r, hivmigration_infants i 
            where r.source_infant_id = i.source_infant_id
              and r.sample_id is not null and r.result is not null;
        ''')
    }

    @Override
    def void revert() {
        if(tableExists("hivmigration_pcr_tests")) {
            executeMysql("delete from obs where encounter_id in (select encounter_id from hivmigration_pcr_tests)")
            executeMysql("delete from encounter where encounter_id in (select encounter_id from hivmigration_pcr_tests)")
            executeMysql("drop table hivmigration_pcr_tests")
        }
    }
}
