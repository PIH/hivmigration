package org.pih.hivmigration.etl.sql;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * Main class for executing the HIV migration using Spark
 */
@Parameters(separators = "=")
public class Migrator {

    private static final Log log = LogFactory.getLog(Migrator.class);

    @Parameter(names={"--step", "-s"}, description="Only run a single step. Should be the prefix of a Migrator, like 'User'.")
    private String step;

    @Parameter(names={"--revert", "-r"}, description="Revert previous changes before the corresponding step.")
    private boolean shouldRevert = false;

    @Parameter(names={"--revert-only", "-e"}, description="Only revert previous changes, don't run the migration. Works with --step.")
    private boolean shouldRevertOnly = false;

    @Parameter(names={"--limit", "-l"}, description="Limit the number of rows to import.")
    private int limit = -1;

    @Parameter(names={"--de-identify", "-d"}, description="De-identify patient names.")
    private boolean deIdentify = false;

    @Parameter(names={"--help", "-h"}, help = true)
    private boolean help = false;

    /**
     * Run the application
     */
	public static void main(String[] args) {
        log.info("Starting up HIV Migrator");
        Migrator migrator = new Migrator();
        JCommander jc = JCommander.newBuilder()
                .addObject(migrator)
                .build();
        jc.parse(args);
        if (migrator.help) {
            jc.usage();
            System.exit(0);
        }
        migrator.main();
    }

    public void main() {
	    if (step != null && !step.isEmpty()) {
	        log.info("Only running " + step + "Migrator.");
        }
	    if (shouldRevert) {
	        log.info("Will revert before running.");
        }
	    if (shouldRevertOnly) {
	        log.info("Reverting only; not migrating.");
        }
        if (limit != -1) {
            log.info("Only transferring " + limit + " rows at each step.");
        }
        if (step != null && !step.isEmpty()) {
            String className = "org.pih.hivmigration.etl.sql." + step + "Migrator";
            try {
                Class cls = Class.forName(className);
                SqlMigrator clsInstance = (SqlMigrator) cls.getDeclaredConstructor().newInstance();
                if (shouldRevert || shouldRevertOnly) {
                    revert(clsInstance);
                }
                if (!shouldRevertOnly) {
                    migrate(new Setup(), -1);
                    migrate(clsInstance, limit);
                }
            } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                log.error("Invalid value for argument --step provided: '" + step
                        + "'. If provided, it must be the prefix to a migrator name, e.g. 'User'.");
                log.error(e);
                System.exit(1);
            }
        } else {
            if (shouldRevert || shouldRevertOnly) {
                revert(new VisitMigrator());
                revert(new TreatmentObsMigrator());
                revert(new ObsLoadingMigrator());
                revert(new MedpickupsMigrator());
                revert(new VitalsMigrator());
                revert(new PcrTestsMigrator());
                revert(new ExamLabResultsMigrator());
                revert(new ExamExtraMigrator());
                revert(new LabResultMigrator());
                revert(new EncounterMigrator());
                revert(new ProviderMigrator());
                revert(new ProgramMigrator());
                revert(new StagingDataMigrator());
                revert(new RegistrationMigrator());
                revert(new InfantMigrator());
                revert(new PatientMigrator());
                revert(new StagingTablesMigrator());
                revert(new UserMigrator());
                revert(new Setup());
            }
            if (!shouldRevertOnly) {
                migrate(new Setup(), -1);
                migrate(new UserMigrator(), -1);
                migrate(new StagingTablesMigrator(), -1);
                migrate(new PatientMigrator(), limit);
                migrate(new InfantMigrator(), limit);
                migrate(new RegistrationMigrator(), limit);
                migrate(new StagingDataMigrator(), -1);
                migrate(new ProgramMigrator(), limit);
                migrate(new ProviderMigrator(), limit);
                migrate(new EncounterMigrator(), limit);
                migrate(new LabResultMigrator(), limit);
                migrate(new VitalsMigrator(), limit);
                migrate(new MedpickupsMigrator(), limit);
                migrate(new ExamExtraMigrator(), limit);
                migrate(new ExamLabResultsMigrator(), limit);
                migrate(new PcrTestsMigrator(), limit);
                migrate(new ObsLoadingMigrator(), limit);
                migrate(new TreatmentObsMigrator(), limit);
                migrate(new VisitMigrator(), limit);
                migrate(new DataWarningsMigrator(), limit);
                if (deIdentify) {
                    migrate(new DeIdentifyMigrator(), -1);
                }
            }
        }
    }

    public static void migrate(SqlMigrator migrator, int limit) {
	    log.info("Executing migrator: " + migrator);
	    migrator.setRowLimit(limit);
        StopWatch sw = new StopWatch();
        sw.start();
        migrator.migrate();
        sw.stop();
        log.info("Migrator " + migrator + " completed in " + sw.toString());
    }

    public static void revert(SqlMigrator migrator) {
        log.info("Executing revert for migrator: " + migrator);
        StopWatch sw = new StopWatch();
        sw.start();
        migrator.revert();
        sw.stop();
        log.info("Migrator " + migrator + " revertion completed in " + sw.toString());
    }
}
