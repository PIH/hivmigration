package org.pih.hivmigration.etl.sql

class DeIdentifyMigrator extends SqlMigrator {

    @Override
    void migrate() {

        executeMysql("De-identifying names",
            '''
             update person_name pn, hivmigration_patients p
            set family_name =
                (CASE
                    WHEN p.source_patient_id % 20 = 0 THEN 'Miranda'
                    WHEN p.source_patient_id % 20 = 1 THEN 'Allen'
                    WHEN p.source_patient_id % 20 = 2 THEN 'Virguna'
                    WHEN p.source_patient_id % 20 = 3 THEN 'Kamikazi'
                    WHEN p.source_patient_id % 20 = 4 THEN 'Inka'
                    WHEN p.source_patient_id % 20 = 5 THEN 'Kichura'
                    WHEN p.source_patient_id % 20 = 6 THEN 'Mukatete'
                    WHEN p.source_patient_id % 20 = 7 THEN 'Seaton'
                    WHEN p.source_patient_id % 20 = 8 THEN 'Ball'
                    WHEN p.source_patient_id % 20 = 9 THEN 'Soucy'
                    WHEN p.source_patient_id % 20 = 10 THEN 'Ioan'
                    WHEN p.source_patient_id % 20 = 11 THEN 'Munson'
                    WHEN p.source_patient_id % 20 = 12 THEN 'Kim'
                    WHEN p.source_patient_id % 20 = 13 THEN 'White'
                    WHEN p.source_patient_id % 20 = 14 THEN 'Cardoza'
                    WHEN p.source_patient_id % 20 = 15 THEN 'Istenes'
                    WHEN p.source_patient_id % 20 = 16 THEN 'Dylan'
                    WHEN p.source_patient_id % 20 = 17 THEN 'Young'
                    WHEN p.source_patient_id % 20 = 18 THEN 'Brady'
                    WHEN p.source_patient_id % 20 = 19 THEN 'Lannister'
                END)
            where family_name != 'UNKNOWN' and pn.person_id = p.person_id;
            
            update person_name pn, hivmigration_patients p
            set given_name =
                (CASE
                    WHEN p.source_patient_id % 19 = 0 THEN 'Lucia'
                    WHEN p.source_patient_id % 19 = 1 THEN 'Maria'
                    WHEN p.source_patient_id % 19 = 2 THEN 'Martina'
                    WHEN p.source_patient_id % 19 = 3 THEN 'Daniela'
                    WHEN p.source_patient_id % 19 = 4 THEN 'Alba'
                    WHEN p.source_patient_id % 19 = 5 THEN 'Ella'
                    WHEN p.source_patient_id % 19 = 6 THEN 'Noemi'
                    WHEN p.source_patient_id % 19 = 7 THEN 'Louise'
                    WHEN p.source_patient_id % 19 = 8 THEN 'Mia'
                    WHEN p.source_patient_id % 19 = 9 THEN 'Roseline'
                    WHEN p.source_patient_id % 19 = 10 THEN 'Madeline'
                    WHEN p.source_patient_id % 19 = 11 THEN 'Mirlande'
                    WHEN p.source_patient_id % 19 = 12 THEN 'Fabienne'
                    WHEN p.source_patient_id % 19 = 13 THEN 'Islande'
                    WHEN p.source_patient_id % 19 = 14 THEN 'Emma'
                    WHEN p.source_patient_id % 19 = 15 THEN 'Olivia'
                    WHEN p.source_patient_id % 19 = 16 THEN 'Ava'
                    WHEN p.source_patient_id % 19 = 17 THEN 'Sophia'
                    WHEN p.source_patient_id % 19 = 18 THEN 'Charlotte'
                END)
            where given_name != 'UNKNOWN' and pn.person_id = p.person_id and p.gender = 'F';
            
            update person_name pn, hivmigration_patients p
            set given_name =
                (CASE
                    WHEN p.source_patient_id % 19 = 0 THEN 'Liam'
                    WHEN p.source_patient_id % 19 = 1 THEN 'Noah'
                    WHEN p.source_patient_id % 19 = 2 THEN 'William'
                    WHEN p.source_patient_id % 19 = 3 THEN 'James'
                    WHEN p.source_patient_id % 19 = 4 THEN 'Oliver'
                    WHEN p.source_patient_id % 19 = 5 THEN 'Gabriel'
                    WHEN p.source_patient_id % 19 = 6 THEN 'Raphael'
                    WHEN p.source_patient_id % 19 = 7 THEN 'Alexandre'
                    WHEN p.source_patient_id % 19 = 8 THEN 'Mohamed'
                    WHEN p.source_patient_id % 19 = 9 THEN 'Louis'
                    WHEN p.source_patient_id % 19 = 10 THEN 'Augustin'
                    WHEN p.source_patient_id % 19 = 11 THEN 'Mark'
                    WHEN p.source_patient_id % 19 = 12 THEN 'Alejandro'
                    WHEN p.source_patient_id % 19 = 13 THEN 'Hugo'
                    WHEN p.source_patient_id % 19 = 14 THEN 'Pablo'
                    WHEN p.source_patient_id % 19 = 15 THEN 'Martin'
                    WHEN p.source_patient_id % 19 = 16 THEN 'Innocent'
                    WHEN p.source_patient_id % 19 = 17 THEN 'Alvaro'
                    WHEN p.source_patient_id % 19 = 18 THEN 'Lucas'
                END)
            where given_name != 'UNKNOWN' and pn.person_id = p.person_id and p.gender != 'F';
            
            update person_name pn, hivmigration_infants p
            set family_name =
                (CASE
                    WHEN p.source_infant_id % 20 = 0 THEN 'Miranda'
                    WHEN p.source_infant_id % 20 = 1 THEN 'Allen'
                    WHEN p.source_infant_id % 20 = 2 THEN 'Virguna'
                    WHEN p.source_infant_id % 20 = 3 THEN 'Kamikazi'
                    WHEN p.source_infant_id % 20 = 4 THEN 'Inka'
                    WHEN p.source_infant_id % 20 = 5 THEN 'Kichura'
                    WHEN p.source_infant_id % 20 = 6 THEN 'Mukatete'
                    WHEN p.source_infant_id % 20 = 7 THEN 'Seaton'
                    WHEN p.source_infant_id % 20 = 8 THEN 'Ball'
                    WHEN p.source_infant_id % 20 = 9 THEN 'Soucy'
                    WHEN p.source_infant_id % 20 = 10 THEN 'Ioan'
                    WHEN p.source_infant_id % 20 = 11 THEN 'Munson'
                    WHEN p.source_infant_id % 20 = 12 THEN 'Kim'
                    WHEN p.source_infant_id % 20 = 13 THEN 'White'
                    WHEN p.source_infant_id % 20 = 14 THEN 'Cardoza'
                    WHEN p.source_infant_id % 20 = 15 THEN 'Istenes'
                    WHEN p.source_infant_id % 20 = 16 THEN 'Dylan'
                    WHEN p.source_infant_id % 20 = 17 THEN 'Young'
                    WHEN p.source_infant_id % 20 = 18 THEN 'Brady'
                    WHEN p.source_infant_id % 20 = 19 THEN 'Lannister'
                END)
            where family_name != 'UNKNOWN' and pn.person_id = p.person_id;
            
            update person_name pn, hivmigration_infants p
            set given_name =
                (CASE
                    WHEN p.source_infant_id % 19 = 0 THEN 'Lucia'
                    WHEN p.source_infant_id % 19 = 1 THEN 'Maria'
                    WHEN p.source_infant_id % 19 = 2 THEN 'Martina'
                    WHEN p.source_infant_id % 19 = 3 THEN 'Daniela'
                    WHEN p.source_infant_id % 19 = 4 THEN 'Alba'
                    WHEN p.source_infant_id % 19 = 5 THEN 'Ella'
                    WHEN p.source_infant_id % 19 = 6 THEN 'Noemi'
                    WHEN p.source_infant_id % 19 = 7 THEN 'Louise'
                    WHEN p.source_infant_id % 19 = 8 THEN 'Mia'
                    WHEN p.source_infant_id % 19 = 9 THEN 'Roseline'
                    WHEN p.source_infant_id % 19 = 10 THEN 'Madeline'
                    WHEN p.source_infant_id % 19 = 11 THEN 'Mirlande'
                    WHEN p.source_infant_id % 19 = 12 THEN 'Fabienne'
                    WHEN p.source_infant_id % 19 = 13 THEN 'Islande'
                    WHEN p.source_infant_id % 19 = 14 THEN 'Emma'
                    WHEN p.source_infant_id % 19 = 15 THEN 'Olivia'
                    WHEN p.source_infant_id % 19 = 16 THEN 'Ava'
                    WHEN p.source_infant_id % 19 = 17 THEN 'Sophia'
                    WHEN p.source_infant_id % 19 = 18 THEN 'Charlotte'
                END)
            where given_name != 'UNKNOWN' and pn.person_id = p.person_id and p.gender = 'F';
           
            update person_name pn, hivmigration_infants p
            set given_name =
                (CASE
                    WHEN p.source_infant_id % 19 = 0 THEN 'Liam'
                    WHEN p.source_infant_id % 19 = 1 THEN 'Noah'
                    WHEN p.source_infant_id % 19 = 2 THEN 'William'
                    WHEN p.source_infant_id % 19 = 3 THEN 'James'
                    WHEN p.source_infant_id % 19 = 4 THEN 'Oliver'
                    WHEN p.source_infant_id % 19 = 5 THEN 'Gabriel'
                    WHEN p.source_infant_id % 19 = 6 THEN 'Raphael'
                    WHEN p.source_infant_id % 19 = 7 THEN 'Alexandre'
                    WHEN p.source_infant_id % 19 = 8 THEN 'Mohamed'
                    WHEN p.source_infant_id % 19 = 9 THEN 'Louis'
                    WHEN p.source_infant_id % 19 = 10 THEN 'Augustin'
                    WHEN p.source_infant_id % 19 = 11 THEN 'Mark'
                    WHEN p.source_infant_id % 19 = 12 THEN 'Alejandro'
                    WHEN p.source_infant_id % 19 = 13 THEN 'Hugo'
                    WHEN p.source_infant_id % 19 = 14 THEN 'Pablo'
                    WHEN p.source_infant_id % 19 = 15 THEN 'Martin'
                    WHEN p.source_infant_id % 19 = 16 THEN 'Innocent'
                    WHEN p.source_infant_id % 19 = 17 THEN 'Alvaro'
                    WHEN p.source_infant_id % 19 = 18 THEN 'Lucas'
                END)
            where given_name != 'UNKNOWN' and pn.person_id = p.person_id and (p.gender != 'F' or p.gender is null);
            
            
            update person_name pn, person p
            set pn.middle_name =
                (CASE
                    WHEN p.person_id % 19 = 0 THEN 'Charlotte'
                    WHEN p.person_id % 19 = 1 THEN 'Sophia'
                    WHEN p.person_id % 19 = 2 THEN 'Ava'
                    WHEN p.person_id % 19 = 3 THEN 'Olivia'
                    WHEN p.person_id % 19 = 4 THEN 'Emma'
                    WHEN p.person_id % 19 = 5 THEN 'Roseline'
                    WHEN p.person_id % 19 = 6 THEN 'Fabienne'
                    WHEN p.person_id % 19 = 7 THEN 'Mirlande'
                    WHEN p.person_id % 19 = 8 THEN 'Madeline'
                    WHEN p.person_id % 19 = 9 THEN 'Islande'
                    WHEN p.person_id % 19 = 10 THEN 'Mia'
                    WHEN p.person_id % 19 = 11 THEN 'Louise'
                    WHEN p.person_id % 19 = 12 THEN 'Noemi'
                    WHEN p.person_id % 19 = 13 THEN 'Ella'
                    WHEN p.person_id % 19 = 14 THEN 'Alba'
                    WHEN p.person_id % 19 = 15 THEN 'Daniela'
                    WHEN p.person_id % 19 = 16 THEN 'Martina'
                    WHEN p.person_id % 19 = 17 THEN 'Maria'
                    WHEN p.person_id % 19 = 18 THEN 'Lucia'
                END)
            where pn.middle_name is not null and pn.middle_name != 'UNKNOWN' 
                and pn.person_id = p.person_id and p.gender = 'F' 
                and (p.person_id in (select person_id from hivmigration_patients) 
                    || p.person_id in (select person_id from hivmigration_infants));
                    
            update person_name pn, person p
            set pn.middle_name =
                (CASE
                    WHEN p.person_id % 19 = 0 THEN 'Lucas'
                    WHEN p.person_id % 19 = 1 THEN 'Alvaro'
                    WHEN p.person_id % 19 = 2 THEN 'Innocent'
                    WHEN p.person_id % 19 = 3 THEN 'Martin'
                    WHEN p.person_id % 19 = 4 THEN 'Pablo'
                    WHEN p.person_id % 19 = 5 THEN 'Hugo'
                    WHEN p.person_id % 19 = 6 THEN 'Alejandro'
                    WHEN p.person_id % 19 = 7 THEN 'Mark'
                    WHEN p.person_id % 19 = 8 THEN 'Augustin'
                    WHEN p.person_id % 19 = 9 THEN 'Louis'
                    WHEN p.person_id % 19 = 10 THEN 'Mohamed'
                    WHEN p.person_id % 19 = 11 THEN 'Alexandre'
                    WHEN p.person_id % 19 = 12 THEN 'Paul'
                    WHEN p.person_id % 19 = 13 THEN 'Peter'
                    WHEN p.person_id % 19 = 14 THEN 'Octavian'
                    WHEN p.person_id % 19 = 15 THEN 'Daniel'
                    WHEN p.person_id % 19 = 16 THEN 'Marvin'
                    WHEN p.person_id % 19 = 17 THEN 'Mario'
                    WHEN p.person_id % 19 = 18 THEN 'Luciano'
                END)
            where pn.middle_name is not null and pn.middle_name != 'UNKNOWN' 
                and pn.person_id = p.person_id and p.gender != 'F' 
                and (p.person_id in (select person_id from hivmigration_patients) 
                    || p.person_id in (select person_id from hivmigration_infants));        
            
            '''
        )

    }

    @Override
    void revert() {
        // nothing to do
    }

}
