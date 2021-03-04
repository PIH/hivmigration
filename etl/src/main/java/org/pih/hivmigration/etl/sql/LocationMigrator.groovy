package org.pih.hivmigration.etl.sql

class LocationMigrator extends SqlMigrator {

    void migrate() {
        executeMysql("Create hivmigration_health_center table", '''
            create table hivmigration_health_center (
            	hiv_emr_id int PRIMARY KEY,
                openmrs_id int NOT NULL
            );
            
            # Unknown location
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 0, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # Centre de Santé Saint-Michel de Boucan-Carré
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 3, location_id from location where uuid = '9bc65df6-99a2-4d43-b56b-f42bde3f5d6d';
            
            # Hôpital Bon Sauveur de Cange
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 4, location_id from location where uuid = '328f68e4-0370-102d-b0e3-001ec94a0cc';
            
            # Hôpital Notre-Dame de la Nativité de Belladère
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 5, location_id from location where uuid = 'd6dafa1a-9a2f-4f61-a33e-410acb64b0e9';
            
            # Hôpital Sainte-Thérèse de Hinche
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 6, location_id from location where uuid = '328f6a60-0370-102d-b0e3-001ec94a0cc1';
            
            # Centre de Santé de Thomonde
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 7, location_id from location where uuid = '376b3e7e-f7c0-4268-a98d-c2bddfee8bcf';
            
            # Hôpital la Colline de Lascahobas
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 8, location_id from location where uuid = '23e7bb0d-51f9-4d5f-b34b-2fbbfeea1960';
            
            # Centre de Santé de Cerca la Source
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 21, location_id from location where uuid = 'c741e25d-eb07-4efc-89e4-38ac73948ae1';
            
            # SSPE de Saint-Marc
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 22, location_id from location where uuid = '209c84cf-4cd7-4908-a946-030499c1ff75';
            
            # Hôpital Universitaire de Mirebalais
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 33, location_id from location where uuid = 'a084f714-a536-473b-94e6-ec317b152b43';
            
            # Centre Médical Charles Colimon de Petite Riviere de l''Artibonite
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 37, location_id from location where uuid = 'a7af173a-f623-445a-bf01-e6a64c0b2c98';
            
            # Unknown Location
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 42, location_id from location where uuid = '8d6c993e-c2cc-11de-8d13-0010c6dffd0f';
            
            # Hôpital Dumarsais Estimé de Verrettes
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 63, location_id from location where uuid = '6c1c2579-7d16-4d39-bca2-e77141ff435f';
            
            # Hôpital Saint-Nicolas de Saint-Marc
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
            select 82, location_id from location where uuid = '97c241a0-eaec-11e5-a837-0800200c9a66';
            
            # Promotion Objectif Zerosida (POZ)
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 40, location_id from location where uuid='c488ed05-f259-4f7b-a9d4-8f56736da691'; 
              
             # TODO Maissade -- this shows as null in HIV Legacy system as March 4, 2021 , so we are mapping to Unknown location
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 36, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
              
            # TODO Jean Denis (?) -- this shows as null in HIV Legacy system as March 4, 2021, so we are mapping to Unknown location
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 122, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f';     
            
            ## TODO:
            ## Review all of the below and change to valid locations as appropriate in OpenMRS
            ## Also review which of these locations might be able to be deleted or merged in HIV EMR
            
            # TODO Cerca cavajal
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 31, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Thomassique
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 32, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Savanette
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 34, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Baptiste
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 35, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
                        
            
            # TODO Tilory
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 38, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Dufailly
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 39, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
                        
            
            # TODO Jean Denis
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 123, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Jean Denis
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 142, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Thomassique
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 162, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f'; 
            
            # TODO Cerca Cavarjal
            insert into hivmigration_health_center(hiv_emr_id, openmrs_id)
              select 163, location_id from location where uuid='8d6c993e-c2cc-11de-8d13-0010c6dffd0f';             
        ''')
    }

    void revert() {
        executeMysql("DROP TABLE IF EXISTS hivmigration_health_center;")
    }
}
