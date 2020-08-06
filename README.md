hivmigration
============

This project represents the code necessary to export HIV data into a format suitable for import into another system,
including a Java API that can be embedded in the target system to facilitate the import.

## Prerequisites

These steps will setup HIV EMR1, Oracle, Pentaho, and this code: 

1. Install HIV EMR system (both application and Oracle db) running locally using Docker, following these instructions:
https://bitbucket.org/partnersinhealth/hivemr/src/master/docker/

2. Install OpenMRS SDK with a clean database (ie. openmrs_hiv) for Haiti HIV:
   - https://wiki.openmrs.org/display/docs/OpenMRS+SDK#OpenMRSSDK-Installation
   - Use Lespwa-style setup with pih_config: haiti-hiv

3. Get Pentaho Data Integration installed locally (mainly so that you can use Spoon - now called "PDI client" to author jobs).  There should no difficulty with any version (Versions 6, 7, or 8 appear to work).  Download from SourceForge:
https://sourceforge.net/projects/pentaho/

4. Get the hivmigration project cloned and available to use locally:
https://github.com/PIH/hivmigration


## Migrating data

Instructions for executing commands from this java project 

1. Create new directory for HIV migration data and log files (eg. `~/hiv-migration`).
2. Set the environment variable `HIVMIGRATION_HOME` (to e.g. `/home/ball/hiv-migration`).
3. Create file `$HIVMIGRATION_HOME/migration.properties` from
   [etl/src/main/resources/sample-migration.properties](https://github.com/PIH/hivmigration/blob/master/etl/src/main/resources/sample-migration.properties).
   Check that the values are correct for your databases.
4. There are 3 ways to run the migration:
   - IntelliJ:  
     - Run `Migrator.java`
   - Command-line:  
     - Use [ansible deployment playbook](https://bitbucket.org/partnersinhealth/deployment/src/master/playbooks/roles/hiv-migration/)
   - Pentaho Spoon: 
     - Set HIV_MIGRATION_HOME in ~/.kettle/kettle.properties.  The other variables will be set automatically.
     - Add Oracle and MySQL connectors (jar files) into Pentaho `lib/` directory
         - `org.gjt.mm.mysql.Driver` from [MySQL Connector/J 5.1.49](https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.49)
         - No idea about the Oracle one
         




