hivmigration
============

This project represents the code necessary to export HIV data into a format suitable for import into another system,
including a Java API that can be embedded in the target system to facilitate the import.

## Prerequisites

These steps will setup HIV EMR1, Oracle, Pentaho, and this code: 

1. Install HIV EMR system (both application and Oracle db) running locally using Docker, following these instructions:
https://bitbucket.org/partnersinhealth/hivemr/docker

2. Install OpenMRS SDK with a clean database (ie. openmrs_hiv) for Haiti HIV:
   - https://wiki.openmrs.org/display/docs/OpenMRS+SDK#OpenMRSSDK-Installation
   - Use Lespwa-style setup with pih_config: haiti-hiv

3. Get Pentaho Data Integration installed locally (mainly so that you can use Spoon - now called "PDI client" to author jobs).  There should no difficulty with any version (Versions 6, 7, or 8 appear to work).  Download from SourceForge:
https://sourceforge.net/projects/pentaho/

4. Get the hivmigration project cloned and available to use locally:
https://github.com/PIH/hivmigration


## Migrating data

Instructions for executing commands from this java project 

1. Create new directory for HIV migration data and log files (ie. $HOME/environment/hiv-migration)
2. Permanently set the environment variable in $HOME/.profile (ie. export HIVMIGRATION_HOME=/home/ball/environment/hiv-migration).  If you do this, you can likely skip setting this later in Step 5.
3. Create file $HIV_MIGRATION_HOME/migration.properties from https://github.com/PIH/hivmigration/blob/master/etl/src/main/resources/sample-migration.properties.  As necessary, modify database, ports, password, etc for mysql and oracle.
4. Install clean SDK with lespwa pih-config variables which creates a clean database (ie. openmrs-hiv).
5. There are 3 ways to run the migration:
   - IntelliJ:  
     - Run Migrator.java.  
     - This requires setting HIVMIGRATION_HOME environment variable and Java 8 in the "Run..." configuration.
   - Command-line:  
     - Use ansible deployment playbook https://bitbucket.org/partnersinhealth/deployment/src/master/playbooks/roles/hiv-migration/
   - Pentaho Spoon: 
     - Set HIV_MIGRATION_HOME in ~/.kettle/kettle.properties.  The other variables will be set automatically.  
     - Add Oracle and MySQL connectors (jar files) into Pentaho data-integrationX.Y/lib directory




