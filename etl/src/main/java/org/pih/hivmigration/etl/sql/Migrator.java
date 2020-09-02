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

    /**
     * Run the application
     */
	public static void main(String[] args) {
        log.info("Starting up HIV Migrator");
        Migrator migrator = new Migrator();
        JCommander.newBuilder()
                .addObject(migrator)
                .build()
                .parse(args);
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
                revert(new EncounterMigrator());
                revert(new ProviderMigrator());
                revert(new ProgramMigrator());
                revert(new StagingDataMigrator());
                revert(new InfantMigrator());
                revert(new PatientMigrator());
                revert(new UserMigrator());
            }
            if (!shouldRevertOnly) {
                migrate(new UserMigrator(), -1);
                migrate(new PatientMigrator(), limit);
                migrate(new InfantMigrator(), limit);
                migrate(new StagingDataMigrator(), -1);
                migrate(new ProgramMigrator(), limit);
                migrate(new ProviderMigrator(), limit);
                migrate(new EncounterMigrator(), limit);

                if (deIdentify == true) {
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
