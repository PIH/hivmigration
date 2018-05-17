hivmigration
============

This project represents the code necessary to export HIV data into a format suitable for import into another system,
including a Java API that can be embedded in the target system to facilitate the import.

Setup:

1. Install HIV EMR1 (ACS) and Oracle running locally using Docker, following these instructions:
https://bitbucket.org/partnersinhealth/hivemr-docker/src/master/

2. As part of step 1, install and setup HIV EMR1 (ACS) files on local disk (outside of docker):
https://bitbucket.org/partnersinhealth/hivemr

3. Get Pentaho Data Integration installed locally (mainly so that you can use Spoon - now called "PDI client" to author jobs).  There should no difficulty with any version (Versions 6, 7, or 8 appear to work).
https://help.pentaho.com/Documentation/8.0/Setup/Installation/Tools/PDI_Client_and_plugins

4. Get the hivmigration project cloned and available to use locally:
https://github.com/PIH/hivmigration
