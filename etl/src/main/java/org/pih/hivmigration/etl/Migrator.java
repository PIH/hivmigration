package org.pih.hivmigration.etl;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.logging.FileLoggingEventListener;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;

/**
 * Main class for executing the HIV migration
 */
public class Migrator {

    private static final Log log = LogFactory.getLog(Migrator.class);

    public static final String HIV_MIGRATION_HOME = "HIVMIGRATION_HOME";
    public static final String JOB_NAME = "job.name";
    public static final String LOG_LEVEL = "job.log.level";

    /**
     * Run the application
     */
	public static void main(String[] args) throws Exception {

        log.info("Starting up HIV Migrator");

        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        log.info("JAVA VM: " + runtimeMxBean.getVmName());
        log.info("JAVA VENDOR: " + runtimeMxBean.getSpecVendor());
        log.info("JAVA VERSION: " + runtimeMxBean.getSpecVersion() + " (" + runtimeMxBean.getVmVersion() + ")");
        log.info("JAVA_OPTS: " + runtimeMxBean.getInputArguments());

	    // Initialize environment
        String homeDirProperty = System.getenv(HIV_MIGRATION_HOME);
        File homeDir = new File(homeDirProperty);
        log.info("Home Dir: " + homeDir.getAbsolutePath());

        File logFile = new File(homeDir, "migration.log");
        log.info("LOGGING TO: " + logFile.getAbsolutePath());

        log.info("Initializing Kettle Environment");
        System.setProperty("KETTLE_HOME", homeDirProperty);
        log.info("KETTLE_HOME = " + System.getProperty("KETTLE_HOME"));
        KettleEnvironment.init();

        File kettleDir = new File(homeDir, ".kettle");
        if (!kettleDir.exists()) {
            kettleDir.mkdirs();
        }
        File kettleProperties = new File(kettleDir, "kettle.properties");
        if (kettleProperties.exists()) {
            kettleProperties.delete();
        }
        Properties props = new Properties();
        props.put("HIV_MIGRATION_HOME", homeDir.getAbsolutePath());
        props.store(new FileOutputStream(kettleProperties), "");

        File propertiesFile = new File(homeDir, "migration.properties");
        if (!propertiesFile.exists()) {
            throw new IllegalStateException("Unable to find migration.properties file at: " + homeDir.getAbsolutePath());
        }
        Properties jobProperties = new Properties();
        InputStream is = null;
        try {
            is = FileUtils.openInputStream(propertiesFile);
            jobProperties.load(is);
        }
        finally {
            IOUtils.closeQuietly(is);
        }

        File tmpSourceDir = new File(homeDir, "source");

        try {
            if (tmpSourceDir.exists()) {
                tmpSourceDir.delete();
            }
            tmpSourceDir.mkdirs();

            loadMigrationCode("jobs", tmpSourceDir);

            String jobFileName = jobProperties.getProperty(JOB_NAME, "migrate.kjb");
            File migrationFile = new File(tmpSourceDir, jobFileName);

            JobMeta jobMeta = new JobMeta(migrationFile.getAbsolutePath(), null);

            Properties p = new Properties(); // TODO: Load these from file

            log.info("Setting job parameters");
            String[] declaredParameters = jobMeta.listParameters();
            for (int i = 0; i < declaredParameters.length; i++) {
                String parameterName = declaredParameters[i];
                String description = jobMeta.getParameterDescription(parameterName);
                String parameterValue = jobMeta.getParameterDefault(parameterName);
                if (p.containsKey(parameterName)) {
                    parameterValue = p.getProperty(parameterName);
                }
                log.info("Setting parameter " + parameterName + " to " + parameterValue + " [description: " + description + "]");
                jobMeta.setParameterValue(parameterName, parameterValue);
            }

            Job job = new Job(null, jobMeta);
            job.setLogLevel(LogLevel.valueOf(jobProperties.getProperty(LOG_LEVEL, "BASIC")));

            StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            FileLoggingEventListener logger = new FileLoggingEventListener(job.getLogChannelId(), logFile.getAbsolutePath(), true);
            KettleLogStore.getAppender().addLoggingEventListener(logger);

            try {
                log.info("Starting Migration");
                job.start();  // Start the job thread, which will execute asynchronously
                job.waitUntilFinished(); // Wait until the job thread is finished
            }
            finally {
                KettleLogStore.getAppender().removeLoggingEventListener(logger);
                logger.close();
            }

            stopWatch.stop();

            Result result = job.getResult();

            log.info("***************");
            log.info("Job executed in:  " + stopWatch.toString());
            log.info("Job Result: " + result);
            log.info("***************");
        }
        finally {
            FileUtils.deleteDirectory(tmpSourceDir);
        }
    }

    /**
     * Recursively copy files from resources folder (eg. "jobs") to directory
     */
    public static void loadMigrationCode(String fromPath, File toDir) {
        PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
        try {
            Resource[] resources = resourceResolver.getResources("classpath*:/" + fromPath + "/**/*");
            if (resources != null) {
                for (Resource r : resources) {
                    if (r.exists() && r.isReadable() && r.contentLength() > 0) {
                        String urlPath = r.getURL().getPath();
                        String prefixToLocate = "!/" + fromPath;
                        int startIndex = urlPath.indexOf(prefixToLocate);
                        if (startIndex > 0) {
                            String dirPath = urlPath.substring(startIndex+prefixToLocate.length());
                            File destFile = new File(toDir + dirPath);
                            FileUtils.copyURLToFile(r.getURL(), destFile);
                            log.info("Loaded job file: " + destFile.getAbsolutePath());
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            throw new IllegalStateException("Error loading migration code", e);
        }
    }
}
