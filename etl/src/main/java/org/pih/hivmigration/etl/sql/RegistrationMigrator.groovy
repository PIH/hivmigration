package org.pih.hivmigration.etl.sql

class RegistrationMigrator extends ObsMigrator {

    @Override
    def void migrate() {

        executeMysql("Create Patient Registration encounter Staging Table",
                '''
            create table hivmigration_registration_encounters (
                encounter_id int PRIMARY KEY AUTO_INCREMENT,
                encounter_date date,
                source_patient_id int,
                creator int
            );
        ''')

        executeMysql("Create Patient Birthplace Staging Table",
                '''
            create table hivmigration_patient_birthplace (
                obs_id int PRIMARY KEY AUTO_INCREMENT,
                encounter_id int,
                source_patient_id int,
                department NVARCHAR(100),
                commune NVARCHAR(100),
                section NVARCHAR(100),
                locality NVARCHAR(100)
            );
        ''')

        setAutoIncrement('hivmigration_registration_encounters', '(select max(encounter_id)+1 from encounter)')
        // load registration encounters into staging table
        executeMysql("Load registration encounters into staging table",'''
                    insert into hivmigration_registration_encounters(
                            source_patient_id,
                            encounter_date,
                            creator
                        )
                    select
                        p.source_patient_id,
                        p.patient_created_date,
                        u.user_id as creator
                    from hivmigration_patients p
                    left join
                        hivmigration_users hu on p.patient_created_by = hu.source_user_id
                    left join
                        users u on u.uuid = hu.user_uuid;
            ''')

        setAutoIncrement("hivmigration_patient_birthplace", "(select max(obs_id)+1 from obs)")

        // load patients birthplace into staging table
        loadFromOracleToMySql('''
                    insert into hivmigration_patient_birthplace(
                        source_patient_id,
                        department,
                        commune,
                        section,
                        locality)
                    values (?,?,?,?,?)
            ''',
                '''
                select
                    d.patient_id as source_patient_id,
                    l.department as department,
                    l.commune as commune,
                    l.section_communale as section,
                    nvl(l.locality, d.BIRTH_PLACE) as locality
                from HIV_DEMOGRAPHICS_REAL d left outer join hiv_locality l
                     on d.BIRTH_PLACE_LOCALITY = l.locality_id
                where l.department is not null
                    or l.commune is not null
                    or l.section_communale is not null
                    or l.locality is not null
                    or d.BIRTH_PLACE is not null;
        ''')

        executeMysql("Create Registration encounters",'''

            SET @encounter_type_registration = (select encounter_type_id from encounter_type where uuid = '873f968a-73a8-4f9c-ac78-9f4778b751b6');
            SET @form_registration = (select form_id from form where uuid = '6F6E6FA0-1E99-41A3-9391-E5CB8A127C11');

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
                    r.encounter_date,
                    r.encounter_date,
                    @encounter_type_registration,
                    @form_registration,
                    p.person_id,
                    r.creator,
                    1
            from hivmigration_registration_encounters r, hivmigration_patients p
            where r.source_patient_id = p.source_patient_id;

            update hivmigration_patient_birthplace b
            join hivmigration_patients hp on b.source_patient_id = hp.source_patient_id
            join encounter e on hp.person_id = e.patient_id and e.encounter_type = @encounter_type_registration
            set b.encounter_id = e.encounter_id;
        ''')

        create_tmp_obs_table()

        executeMysql("Load birthplaces as obs",'''

            ALTER TABLE hivmigration_patient_birthplace ADD COLUMN encounter_date DATETIME;

            UPDATE hivmigration_patient_birthplace b
            JOIN hivmigration_registration_encounters e ON b.encounter_id = e.encounter_id
            SET b.encounter_date = e.encounter_date;

            -- Create Birthplace address construct
            INSERT INTO tmp_obs (
                obs_id,
                encounter_id,
                obs_datetime,
                concept_uuid)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Birthplace address construct')
            from    hivmigration_patient_birthplace b
            where locality is not null or section is not null or commune is not null or department is not null;

            -- Add Locality
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Address1'),
                locality
            from    hivmigration_patient_birthplace
            where   locality is not null;

            -- Add Section Communale
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Address3'),
                section
            from    hivmigration_patient_birthplace
            where   section is not null;


            -- Add Commune
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'City Village'),
                commune
            from    hivmigration_patient_birthplace
            where   commune is not null;

            -- Add Department
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'State Province'),
                department
            from    hivmigration_patient_birthplace
            where   department is not null;

            -- Add Country
            INSERT INTO tmp_obs (
                obs_group_id,
                encounter_id,
                obs_datetime,
                concept_uuid,
                value_text)
            select
                obs_id,
                encounter_id,
                encounter_date,
                concept_uuid_from_mapping('PIH', 'Country'),
                'Haiti'
            from    hivmigration_patient_birthplace;
        ''')

        migrate_tmp_obs()

        executeMysql("Add civil status obs", '''
            -- Add CIVIL STATUS
            SET @civil_status_concept = (concept_from_mapping('PIH', 'CIVIL STATUS'));

            INSERT INTO obs (
                    person_id,
                    encounter_id,
                    obs_datetime,
                    location_id,
                    concept_id,
                    value_coded,
                    creator,
                    date_created,
                    uuid)
            select
                    p.person_id,
                    r.encounter_id,
                    r.encounter_date as obs_datetime,
                    1 as locationId,
                    @civil_status_concept as concept_id,
                    CASE UPPER(TRIM(s.civil_status))
                        WHEN 'DIVORCED' THEN concept_from_mapping('PIH', 'DIVORCED')
                        WHEN 'MARRIED' THEN concept_from_mapping('PIH', 'MARRIED')
                        WHEN 'PARTNER' THEN concept_from_mapping('PIH', 'LIVING WITH PARTNER')
                        WHEN 'PLACE' THEN concept_from_mapping('PIH', 'LIVING WITH PARTNER')
                        WHEN 'SEPARATED' THEN concept_from_mapping('PIH', 'SEPARATED')
                        WHEN 'SINGLE' THEN concept_from_mapping('PIH', 'SINGLE OR A CHILD')
                        WHEN 'WIDOWED' THEN concept_from_mapping('PIH', 'WIDOWED')
                        ELSE concept_from_mapping('PIH', 'OTHER')
                    END as value_coded,
                    r.creator, r.encounter_date as dateCreated, uuid() as uuid
            from hivmigration_socioeconomics s, hivmigration_registration_encounters r, hivmigration_patients p
            where  s.civil_status is not null and s.source_patient_id = r.source_patient_id and r.source_patient_id = p.source_patient_id;
        ''')

        executeMysql("Create temp table for mapping ZL patients job titles", '''
            CREATE TABLE hivmigration_zl_jobs_mapping (
                job_id INT PRIMARY KEY AUTO_INCREMENT,
                hiv_job_entry VARCHAR(38),
                zl_job_title VARCHAR(48),
                openmrs_concept_source VARCHAR(32),
                openmrs_concept_code VARCHAR(64)
            );
        ''')

        executeMysql("Add HIV Jobs mappings", '''
            insert into hivmigration_zl_jobs_mapping(
                hiv_job_entry,
                zl_job_title,
                openmrs_concept_source,
                openmrs_concept_code) 
            values
                ('activités domestiques', 'ménagère', 'PIH', 'Cleaner'),
                ('agent de change', 'agent de change', 'PIH', 'Stockbroker'),
                ('agent de securite', 'agent de sécurité', 'PIH', 'Guard'),
                ('agent securitã‰', 'agent de sécurité', 'PIH', 'Guard'),
                ('agriculteur', 'cultivateur', 'PIH', 'FARMER'),
                ('agriculture-soudure', 'cultivateur', 'PIH', 'FARMER'),
                ('agronome', 'agronome', 'PIH', 'Agronomist'),
                ('ale lekol', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('anplaye faktori', 'manufacturier/ère', 'PIH', 'FACTORY WORKER'),
                ('apanteur', 'arpenteur', 'PIH', 'Surveyor'),
                ('archiviste', 'archiviste', 'PIH', 'Archivist'),
                ('arpenteur', 'arpenteur', 'PIH', 'Surveyor'),
                ('artisanat', 'artisant de produits locaux', 'PIH', 'Artisan'),
                ('artisant', 'artisant de produits locaux', 'PIH', 'Artisan'),
                ('artiste', 'artiste/peintre', 'PIH', 'Artist'),
                ('artiste peintre', 'artiste/peintre', 'PIH', 'Artist'),
                ('assistant promoteur sante', 'assistante administrative/secrétaire', 'PIH', 'Secretary'),
                ('assistante', 'assistante administrative/secrétaire', 'PIH', 'Secretary'),
                ('aucun', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('aucune', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('auxiliaire- inf.', 'auxiliaire-infirmière', 'PIH', 'Nurse aide'),
                ('auxilliaire', 'auxiliaire-infirmière', 'PIH', 'Nurse aide'),
                ('bb', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('bibliothecaire', 'bibliothécaire', 'PIH', 'Librarian'),
                ('blanchisseuse,menage,cuisine', 'ménagère', 'PIH', 'Cleaner'),
                ('boletier', 'borletier', 'PIH', 'Lottery worker'),
                ('borlette', 'borletier', 'PIH', 'Lottery worker'),
                ('bos mason', 'maçon', 'PIH', 'Construction worker'),
                ('boss peintre', 'artiste/peintre', 'PIH', 'Artist'),
                ('boulanger', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('boulangerie', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('broderie', 'créateur/trice de mode', 'PIH', 'Fashion designer'),
                ('buissness', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('business', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('busness', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('busnismen', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('camionneur', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('capitaine batiment', 'majordome', 'PIH', 'Butler'),
                ('cauffeur de camion', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chaje machin', 'aide-chauffeur', 'PIH', 'Assistant driver'),
                ('chaje machine', 'aide-chauffeur', 'PIH', 'Assistant driver'),
                ('chanpentier', 'charpentier', 'PIH', 'Carpenter'),
                ('chany', 'cireur de bottes', 'PIH', 'Shoeshiner'),
                ('chapant', 'charpentier', 'PIH', 'Carpenter'),
                ('chapantye', 'charpentier', 'PIH', 'Carpenter'),
                ('chapentier', 'charpentier', 'PIH', 'Carpenter'),
                ('charpentier', 'charpentier', 'PIH', 'Carpenter'),
                ('chasseur', 'chasseur', 'PIH', 'Hunter'),
                ('chauffeuer', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeuer taxi', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur (trailer)', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur d autobus', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur de camion', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur de taptap', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur de taxi moto', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur moto', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur pds lourds', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur taxi moto', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur, mecanicien', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chauffeur,peintre', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chofãˆ', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chofe taxi', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('choffeur', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('chomeuse', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('cireur de botte', 'cireur de bottes', 'PIH', 'Shoeshiner'),
                ('commedien/ecolier', 'elève', 'PIH', 'STUDENT'),
                ('commerã‡ante', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('commerce', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('comptabilite /politicien', 'comptable', 'PIH', 'Accountant'),
                ('comptable', 'comptable', 'PIH', 'Accountant'),
                ('conducteur', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('conducteur d\\'automobile', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('conducteur de camion', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('confection de selle de chevaux', 'artisant de produits locaux', 'PIH', 'Artisan'),
                ('controle des marchandises', 'vendeur', 'PIH', 'Vendor'),
                ('controleur, chauffeur', 'aide-chauffeur', 'PIH', 'Assistant driver'),
                ('cordonier', 'cordonnier', 'PIH', 'Shoemaker'),
                ('cordonnier', 'cordonnier', 'PIH', 'Shoemaker'),
                ('cosmetologie', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('cosmetologie(estheticienne)', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('cosmetologue', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('couture', 'tailleur/couturière', 'PIH', 'Tailor'),
                ('couturiere', 'tailleur/couturière', 'PIH', 'Tailor'),
                ('cuisine /patisserie', 'chef de cuisine', 'PIH', 'Cook'),
                ('cuisiniere', 'chef de cuisine', 'PIH', 'Cook'),
                ('cultivateur', 'cultivateur', 'PIH', 'FARMER'),
                ('driver', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('ebenis', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('ebeniste', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('ebenistene', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('ebenistre', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('ebiste', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('ecole', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('ecole de couture', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('ecolier', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('ecoliere', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('educateur', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('educatrice', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('electricien', 'electricien', 'PIH', 'Electrician'),
                ('electricite /plomberie', 'electricien', 'PIH', 'Electrician'),
                ('elektricien', 'electricien', 'PIH', 'Electrician'),
                ('eleve', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('enseigante', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('enseignant', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('enseignante', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('enseignat', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('enseignent', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('esteticienne', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('estheticienne', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('etidyan', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('etudiant', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('etudiante', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('farmer', 'cultivateur', 'PIH', 'FARMER'),
                ('fe bolet', 'borletier', 'PIH', 'Lottery worker'),
                ('fe lesiv', 'ménagère', 'PIH', 'Cleaner'),
                ('fe manje', 'ménagère', 'PIH', 'Cleaner'),
                ('femme de menage', 'ménagère', 'PIH', 'Cleaner'),
                ('feraye', 'boss de soudure', 'PIH', 'Welder'),
                ('feronerie', 'boss de soudure', 'PIH', 'Welder'),
                ('feronnerie', 'boss de soudure', 'PIH', 'Welder'),
                ('ferrailleur', 'boss de soudure', 'PIH', 'Welder'),
                ('ferronerie', 'boss de soudure', 'PIH', 'Welder'),
                ('ferronier', 'boss de soudure', 'PIH', 'Welder'),
                ('ferronnerie', 'boss de soudure', 'PIH', 'Welder'),
                ('fleuriste', 'fleuriste', 'PIH', 'Florist'),
                ('forgeron', 'boss de soudure', 'PIH', 'Welder'),
                ('forman', 'forman', 'PIH', 'Foreman'),
                ('former_soldier', 'retraité', 'PIH', 'RETIRED'),
                ('garde du corps', 'garde du corps', 'PIH', 'Bodyguard'),
                ('gardien', 'majordome', 'PIH', 'Butler'),
                ('greffier', 'greffier', 'PIH', 'Clerk'),
                ('hougan', 'leader communautaire (hougan/manbo/ pasteur)', 'PIH', 'Community leader'),
                ('huissier', 'huissier', 'PIH', 'Bailiff'),
                ('huissier du tribunal', 'huissier', 'PIH', 'Bailiff'),
                ('insp de douane', 'inspecteur de douane', 'PIH', 'Customs inspector'),
                ('institutrice', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('institutrice ecole primaire', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('intitutrice', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('jardin, travail bois', 'ebéniste', 'PIH', 'Cabinetmaker'),
                ('jardiniãˆre d\\'enfant', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('jardiniere', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('kontrole machin', 'aide-chauffeur', 'PIH', 'Assistant driver'),
                ('kouti', 'tailleur/couturière', 'PIH', 'Tailor'),
                ('la peche', 'pêcheur', 'PIH', 'Fisherman'),
                ('l\\'ecole', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('lekol', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('lesiviere', 'lessivière', 'PIH', 'Laundry worker'),
                ('lessive', 'lessivière', 'PIH', 'Laundry worker'),
                ('lessiveuse', 'lessivière', 'PIH', 'Laundry worker'),
                ('lessiviere (parent)', 'lessivière', 'PIH', 'Laundry worker'),
                ('li travay nan boulanger', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('maã‡onnerie', 'maçon', 'PIH', 'Construction worker'),
                ('machann bolet', 'borletier', 'PIH', 'Lottery worker'),
                ('macon', 'maçon', 'PIH', 'Construction worker'),
                ('main d\\'oeuvre', 'manufacturier/ère', 'PIH', 'FACTORY WORKER'),
                ('mambo', 'leader communautaire (hougan/manbo/ pasteur)', 'PIH', 'Community leader'),
                ('manual_labor', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('manual_work', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('maoganie', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('marcon', 'maçon', 'PIH', 'Construction worker'),
                ('marine', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('mason', 'maçon', 'PIH', 'Construction worker'),
                ('masson', 'maçon', 'PIH', 'Construction worker'),
                ('matron', 'matronne', 'PIH', 'Matron'),
                ('mecanicien', 'mécanicien', 'PIH', 'Mechanic'),
                ('mecanicien/chauffeur', 'mécanicien', 'PIH', 'Mechanic'),
                ('mecanicien-chauffeur', 'mécanicien', 'PIH', 'Mechanic'),
                ('mecanique', 'mécanicien', 'PIH', 'Mechanic'),
                ('mekanisyen', 'mécanicien', 'PIH', 'Mechanic'),
                ('menager', 'ménagère', 'PIH', 'Cleaner'),
                ('menagere', 'ménagère', 'PIH', 'Cleaner'),
                ('menuisier', 'menuisier', 'PIH', 'Carpenter'),
                ('messager', 'messager', 'PIH', 'Messenger'),
                ('motocycliste', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('musicien', 'musicien', 'PIH', 'Musician'),
                ('na', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('none', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('normale', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('operateur', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('operatrice factory', 'manufacturier/ère', 'PIH', 'FACTORY WORKER'),
                ('other', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('ougan', 'leader communautaire (hougan/manbo/ pasteur)', 'PIH', 'Community leader'),
                ('ougant', 'leader communautaire (hougan/manbo/ pasteur)', 'PIH', 'Community leader'),
                ('ouvrier', 'ouvrier', 'PIH', 'MANUAL LABORER '),
                ('patisserie', 'pâtissière', 'PIH', 'Pastry chef'),
                ('peche', 'pêcheur', 'PIH', 'Fisherman'),
                ('pecheur', 'pêcheur', 'PIH', 'Fisherman'),
                ('pecheur poisson', 'pêcheur', 'PIH', 'Fisherman'),
                ('peindre', 'artiste/peintre', 'PIH', 'Artist'),
                ('peintre', 'artiste/peintre', 'PIH', 'Artist'),
                ('photographe', 'photographe', 'PIH', 'Photographer'),
                ('pintre', 'artiste/peintre', 'PIH', 'Artist'),
                ('plomberie', 'plombier', 'PIH', 'Plumber'),
                ('plombier', 'plombier', 'PIH', 'Plumber'),
                ('pnh', 'policier  ', 'PIH', 'Police'),
                ('police', 'policier  ', 'PIH', 'Police'),
                ('policier', 'policier  ', 'PIH', 'Police'),
                ('professeur', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('professeur /musicien', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('professeur avocat', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('professeur d\\'ecole', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('pwofese', 'educateur/trice/enseignant/professeur', 'PIH', 'Teacher'),
                ('radiologue', 'radiologue', 'PIH', 'Radiologist'),
                ('refrigerateur', 'responsable de réfrigération', 'PIH', 'Refrigeration manager'),
                ('retraite', 'retraité', 'PIH', 'RETIRED'),
                ('science comptable', 'comptable', 'PIH', 'Accountant'),
                ('scoliere', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('secretaire', 'assistante administrative/secrétaire', 'PIH', 'Secretary'),
                ('securite', 'agent de sécurité', 'PIH', 'Guard'),
                ('sekirite', 'agent de sécurité', 'PIH', 'Guard'),
                ('servante', 'ménagère', 'PIH', 'Cleaner'),
                ('serviteur ouangan', 'leader communautaire (hougan/manbo/ pasteur)', 'PIH', 'Community leader'),
                ('soudure', 'boss de soudure', 'PIH', 'Welder'),
                ('steticienne', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('stheticienne', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('student', 'elève/ etudiant', 'PIH', 'STUDENT'),
                ('studio de beaute', 'esthéticien/ne', 'PIH', 'Beautician'),
                ('superviseur centre d\\'alphabetisation', 'autres (à préciser)', 'PIH', 'MAIN ACTIVITY, NON-CODED'),
                ('tailleur', 'tailleur/couturière', 'PIH', 'Tailor'),
                ('taxi', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('taxi moto', 'chauffeur (taxi/moto/camion)', 'PIH', 'DRIVER'),
                ('tayãˆ', 'tailleur/couturière', 'PIH', 'Tailor'),
                ('technicien agricole', 'agronome', 'PIH', 'Agronomist'),
                ('technicien de laboratoire', 'technicien de laboratoire', 'PIH', 'Laboratory technician'),
                ('travailleur de sexe', 'travailleur de sexe', 'PIH', 'Sex worker'),
                ('travailleur manuel', 'manufacturier/ère', 'PIH', 'FACTORY WORKER'),
                ('travailleuse de sexe', 'travailleur de sexe', 'PIH', 'Sex worker'),
                ('travaiyãˆ machin', 'aide-chauffeur', 'PIH', 'Assistant driver'),
                ('travay nan boulanger', 'commerçant/e', 'PIH', 'COMMERCE'),
                ('travay nan kay', 'ménagère', 'PIH', 'Cleaner'),
                ('unemployed', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('unemployed?', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('mandiante', 'sans occupation/emploi', 'PIH', 'UNEMPLOYED'),
                ('personnel de confiance', 'personnel de confiance', 'PIH', 'Personal assistant'),
                ('conte maitre', 'contre-maitre', 'PIH', 'Foreman'),
                ('ko donye', 'coordonnier', 'PIH', 'Shoemaker'),
                ('koud soulye', 'coordonnier', 'PIH', 'Shoemaker');       
            ''')

        executeMysql("Add OCCUPATION obs", '''
            -- Add CIVIL STATUS
            SET @occupation_concept_id = (concept_from_mapping('PIH', 'Occupation'));
              
            INSERT INTO obs (                    
                    person_id, 
                    encounter_id, 
                    obs_datetime, 
                    location_id, 
                    concept_id,    
                    value_coded,  
                    comments,              
                    creator, 
                    date_created, 
                    uuid)
            SELECT 
                    p.person_id, 
                    r.encounter_id, 
                    r.encounter_date as obs_datetime, 
                    1 as locationId, 
                    @occupation_concept_id as concept_id, 
                    case 
                        when (m.openmrs_concept_code is not null) then concept_from_mapping(m.openmrs_concept_source, m.openmrs_concept_code)
                        else concept_from_mapping('PIH', 'MAIN ACTIVITY, NON-CODED')
                    end as value_coded,  
                    case 
                        when (m.openmrs_concept_code is null) then s.primary_activity -- there is no mapping to this entry, so we are going to migrate it to comments column
                        else null
                    end as comments,  
                    r.creator, r.encounter_date as dateCreated, uuid() as uuid  
            FROM hivmigration_socioeconomics s join hivmigration_patients p on s.source_patient_id = p.source_patient_id 
                join hivmigration_registration_encounters r on p.source_patient_id = r.source_patient_id 
                    left join hivmigration_zl_jobs_mapping m on s.primary_activity = m.hiv_job_entry 
            WHERE s.primary_activity is not null;    
                                                
        ''')
    }

    @Override
    def void revert() {
        if(tableExists("hivmigration_registration_encounters")) {
            executeMysql("delete from obs where (encounter_id in (select encounter_id from hivmigration_registration_encounters)) and (obs_group_id is not null)")
            executeMysql("delete from obs where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("delete from encounter where encounter_id in (select encounter_id from hivmigration_registration_encounters)")
            executeMysql("drop table if exists hivmigration_zl_jobs_mapping")
            executeMysql("drop table if exists hivmigration_patient_birthplace")
            executeMysql("drop table if exists hivmigration_registration_encounters")
        }
    }
}
