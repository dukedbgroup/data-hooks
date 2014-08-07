package edu.duke.dbmsplus.datahooks.execution;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//ToDo: compress by streaming (walk hdfs, add files to streaming tar output, max memory is needed for largest single file) http://commons.apache.org/proper/commons-compress/

//ToDo: using org.apache.hadoop.fs.FileSystem directly to enable parallel actions; note FsShell jar changes

//ToDo: utilize multiple worker threads when multiple aggregations of logsets need to be processed for a catch-up

//ToDo: is xz compression always installed in our Linux targets? Could consider alternative compression method like gz directly in Java.

/** Daemon to archive old query logs and clean up archived files for Hive-Hook.
 * Not thread-safe.
 * Created: 20131015T1116
 *
 * @author pbaclace
 */
public class TidyDir {
    final private static String DU_PARSE_REGEX_PRE =
      "([0-9]+)[ ]+(hdfs://)?([0-9a-z\\.:]*)?";
    final private static String DU_PARSE_REGEX_POST =
          "([/a-zA-Z0-9_-]*)";
    final private static String DIR_NAME_REGEX =
      "([a-zA-Z0-9-]+)_([0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])([/a-fA-F0-9_-]*)";
    final static String tzCodeHive = "America/New_York";//ToDo: get tz name from configuration
   	final static TimeZone tzHive = TimeZone.getTimeZone(tzCodeHive);
    final static String tzCodeArhive = "America/Los_Angeles";//ToDo: get tz name from configuration
   	final static TimeZone tzOut = TimeZone.getTimeZone(tzCodeArhive);
   	final public static java.text.SimpleDateFormat sdfFileQueryIdTimestamp = new java.text.SimpleDateFormat("yyyyMMddHHmm");
    final public static java.text.SimpleDateFormat sdfArchive = new java.text.SimpleDateFormat("yyyy'_'MM'_'dd'T'HHmm");
    final public static java.text.SimpleDateFormat sdfArchivePath = new java.text.SimpleDateFormat("yyyy'/'MM'/'dd");
    final public static java.text.SimpleDateFormat sdfLogOutput = new java.text.SimpleDateFormat("yyyy'/'MM'/'dd'T'HH':'mm':'ss'Z'Z");
   	static {
   	    sdfFileQueryIdTimestamp.setTimeZone(tzHive); // input filename based on hive query id
        sdfArchive.setTimeZone(tzOut); // name of aggregated logsets in a tar file
           sdfArchivePath.setTimeZone(tzOut); // where archive files are stored
       }
    private static final String SELF_IDENT_FILENAME = ".TidyDir";

    private boolean dryRun = false; // if true, then only print out what would change, no side effects
    private boolean noLocalDelete = false; // if true, dont delete files from local tmp area (for debugging)
    private boolean noHdfsDelete = false; // if true, dont delete logsets
    private volatile boolean done = false;
    private long sleepPeriodMsec = 313*1000L;
    private long maxIter = 1000000000L;
    private long iterCount = 0L;
    final private long startTime = System.currentTimeMillis();
    private long maxTmpSpaceBytes = 10L * 1024L * 1024L * 1024L;
    private long stopAfterMsec = 1000000000000L; // 32 years in msec
    private long deleteArchiveAgeMsec = 365L * 24L * 60L * 60L * 1000L;
    private long stopTime = startTime + stopAfterMsec;
    private String hadoopConfPath;
    final private File localTempParent = new File(System.getProperty("java.io.tmpdir", "/tmp"));
    private String logicalDaemonInstance;
    final File localTemp = new File(localTempParent, "unravel_tidydir");

    private File selfIdentFile;

    private ByteArrayOutputStream baos_out = new ByteArrayOutputStream(500000);
    PrintStream ps_out = new PrintStream(baos_out);

    private String hdfsArchivePath;

    private Pattern archivePattern;
    private Pattern dirNamePattern;

    /**
     * model for a single logset that corresponds to a single Hive query.
     */
    public static class LogSetEntry implements Comparable<LogSetEntry> {
        private String dirName;
        private long timestamp; // msec since 1970
        private String hdfsPath;
        private long size; // bytes
        private String userName;

        public LogSetEntry(String dirName, String hdfsPath, long size, long timestamp, String userName) {
            this.dirName = dirName;
            this.hdfsPath = hdfsPath;
            this.size = size;
            this.timestamp = timestamp;
            this.userName = userName;
        }

        @Override
        public int compareTo(LogSetEntry o) {
            LogSetEntry other = o;
            if (this.getTimestamp() < other.getTimestamp()) {
                return -1;
            } else if (this.getTimestamp() > other.getTimestamp()) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof LogSetEntry)) return false;

            LogSetEntry that = (LogSetEntry) o;

            if (!hdfsPath.equals(that.hdfsPath)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return dirName.hashCode();
        }

        public String getDirName() {
            return dirName;
        }

        public String getHdfsPath() {
            return hdfsPath;
        }

        public long getSize() {
            return size;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getUserName() {
            return userName;
        }

        @Override
        public String toString() {
            return "LogSetEntry{" +
                     "dirName='" + dirName + '\'' +
                     ", timestamp=" + timestamp +
                     ", hdfsPath='" + hdfsPath + '\'' +
                     ", size=" + size +
                     ", userName='" + userName + '\'' +
                     '}';
        }
    }

    // command line like:   TidyDir  --period=1800s --max-iter=100 --max-tmp=10g --del-arcs-after=24h --stop-after=24h

    public void printUsage() {
        System.out.println("TidyDir archives analyzed files and deletes archives        ");
        System.out.println("Usage: ");
        System.out.println("       --period=T      # units can be s, m, h, d for seconds (default), minutes, hours, days");
        System.out.println("              #  default is  " + (sleepPeriodMsec/1000L) + " seconds");
        System.out.println("       --max-iter=N    # execute for N periods and then exit");
        System.out.println("              #  use --max-iter=1 to perform one pass");
        System.out.println("              #  default is huge");
        System.out.println("       --stop-after=T  # alternative to --max-iter to stop processing ");
        System.out.println("              # default is several decades ");
        System.out.println("       --max-tmp=D     # use no more than D units of local tmp disk space, ");
        System.out.println("              #  D units  can be m or g for megabytes (default) or gigabytes");
        System.out.println("              #  default is 10g for 10 gigabytes ");
        System.out.println("       --del-arcs-after=T  # delete archived files older than T ");
        System.out.println("              #  based on actual age of logs (not file timestamp)");
        System.out.println("              #  (default is 365d for 1 year [NOT IMPLEMENTED])");
        System.out.println("       --dry-run      # emit to stderr what would happen, but make no changes.");
        System.out.println("       --help      # emit usage.");
        System.out.println("       -help       # emit usage.");
        System.out.println("       -h          # emit usage.");
        System.out.println("Example Usage: (no args)");
        System.out.println("       # wake up every 313sec., archive files using no more than 200 MB of local tmp space, and delete archived files after 30 days");
        System.out.println("Example Usage: --period=20m --stop-after=1h --max-tmp=1g --del-arcs-after=90d");
        System.out.println("       # wake up every 20 min., use up to 1 GB of local tmp space to compress files into archive files, and stop all processing after 1 hour (even if incomplete), ");
        System.out.println("Configuration: use a Java property settings to specify these:");
        System.out.println("       -Dhadoop.conf.dir=/home/hadoop/conf   # default is /etc/hadoop/conf");
        System.out.println("       -D" + HookUtils.HIVE_RESULT_DIR_CONF + "=/somepath   # default is " +
                             HookUtils.DEFAULT_HIVE_RESULT_DIR);

    }

    public static void main(String[] args) {
        TidyDir inst = new TidyDir();
        inst.main2(args);
    }

    public void main2(String[] args) {
        // parse args
        int argOffset=0;
        while (argOffset < args.length) {
            String arg = args[argOffset++];
            if (arg.startsWith("--period=")) {
                sleepPeriodMsec = parseToMSec(arg.substring(9));
            } else if (arg.startsWith("--max-iter=")) {
                maxIter = parseToLong(arg.substring(11));
            } else if (arg.startsWith("--stop-after=")) {
                stopAfterMsec = parseToMSec(arg.substring(13));
            } else if (arg.startsWith("--max-tmp=")) {
                maxTmpSpaceBytes = parseToBytes(arg.substring(10));
            } else if (arg.startsWith("--del-arcs-after=")) {
                deleteArchiveAgeMsec = parseToMSec(arg.substring(17));
            } else if (arg.equals("--dry-run")) {
                dryRun = true;
            } else if (arg.equals("--help") || arg.equals("-help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            } else if (arg.equals("--no-hdfs-delete")) {
                noHdfsDelete =true;
            } else if (arg.equals("--no-local-delete")) {
                noLocalDelete =true;
            } else {
                System.err.println();
                System.err.println("Unknown argument: " + arg);
                System.err.println();
                printUsage();
                System.exit(0);
            }
        }

        hadoopConfPath = System.getProperty("hadoop.conf.dir", "/etc/hadoop");
        String hdfsBasePath = HookUtils.getHiveResultDir(getConf());
        HiveLogLayoutManager hiveLogLayoutManager = new HiveLogLayoutManager(hdfsBasePath);
        String[] pathsToArchive = new String[1];
        pathsToArchive[0] = hiveLogLayoutManager.getTopDirPath(HiveLogLayoutManager.State.ANALYZED);
        String[] pathsToClean = new String[1];
        pathsToClean[0] = hdfsArchivePath =  hiveLogLayoutManager.getTopDirPath(HiveLogLayoutManager.State.ARCHIVED);

        // attempt to create temp area if it does not exist
        if (! localTempParent.exists()) {
            localTempParent.mkdirs();
        }
        localTemp.mkdirs();
        if (! localTemp.isDirectory()) {
            emsg("FATAL: could not create dir: " + localTemp.getAbsolutePath());
            System.exit(3);
        }

        // make sure this is only instance of TidyDir working on this host for particular conf
        //
        logicalDaemonInstance = hdfsBasePath.replace('/', '_');
        selfIdentFile = new File(localTemp, SELF_IDENT_FILENAME + "." + logicalDaemonInstance);
        boolean alreadyRunning = false;
        if (selfIdentFile.exists()) {
            alreadyRunning = true;
        } else {
            try {
                if (! selfIdentFile.createNewFile()) {
                    alreadyRunning = true;
                }
            } catch (IOException e) {
                emsg("FATAL: could not create: " + selfIdentFile.getAbsolutePath());
                System.exit(2);
            }
            selfIdentFile.deleteOnExit();
        }
        if (alreadyRunning) {
            emsg("FATAL: self ident file exists, ensure not already running then remove: " + selfIdentFile.getAbsolutePath());
            System.exit(1);
        }

        archivePattern = Pattern.compile(DU_PARSE_REGEX_PRE + pathsToArchive[0] + "/" + DU_PARSE_REGEX_POST);
        dirNamePattern = Pattern.compile(DIR_NAME_REGEX);

        stopTime = startTime + stopAfterMsec;
        // redirect stdout so we can intercept output of FsShell
        System.setOut(ps_out);

        try {
            tidyLoop(pathsToArchive, pathsToClean);
        } catch (IOException e) {
            String msg = "ERROR: " + e;
            emsg(msg);
        }
    }

    private void emsg(String msg) {
        final Date now = new Date();
        System.err.print(sdfLogOutput.format(now));
        System.err.print(" ");
        System.err.println(msg);
    }

    /**
     *
     * @param pathsToArchive can be zero length.
     * @param pathsToClean can be zero length.
     * @throws IOException if dest dirs cannot be created or written to.
     */
    private void tidyLoop(String[] pathsToArchive, String[] pathsToClean) throws IOException {
        while (!done) {
            // check if it is time to stop
            if (limitReached()) return;

            HookUtils.ensureStateDirsExist(getConf());
            // action
            //
            for (String s : pathsToClean) {
                cleanupDir(s);
            }
            for (String s : pathsToArchive) {
                archiveDir(s);
            }
            iterCount++;
            // check if it is time to stop
            if (limitReached()) return;
            // sleep period
            try {
                Thread.sleep(sleepPeriodMsec);
            } catch (InterruptedException e) {
                if (Thread.interrupted()) {
                    done = true;
                    emsg("Stopping due to interrupt/signal");
                    return;
                }
            }
        }
    }

    private boolean limitReached() {
        if (done) {
            return true;
        }
        if (Thread.interrupted()) {
            done = true;
            return true;
        }
        if (iterCount >= maxIter) {
            done = true;
            emsg("Stopping after " + iterCount + " iterations due to set iteration limit");
            return true;
        }
        if (System.currentTimeMillis() >= stopTime) {
            done = true;
            emsg("Stopping after " + (stopTime/1000L) + " seconds due to run time limit set");
            return true;
        }
        return false;
    }

    /**
     *
     * @param archiveBaseDirHdfs
     */
    private void archiveDir(String archiveBaseDirHdfs) {

        // list /analyzed/ directory
        List<LogSetEntry> list = hdfsDUdir(archiveBaseDirHdfs);

        // sort entries by date
        Collections.sort(list);

        //  state vars for aggregation of entries
        ArrayList<LogSetEntry> aggregate = new ArrayList<LogSetEntry>(50);
        long aggregateSize = 0;
        long maxSizeUncompressed = maxTmpSpaceBytes / 2L;
        Date firstEntryDate = null;
        // use firstEntryYYYYMMDD to aggregate entries for one day only
        String firstEntryYYYYMMDD = null;
        String nowYYYYMMDD = sdfArchivePath.format(new Date());
        Date lastEntryDate = null;
        boolean emitAgg = false;

        for (LogSetEntry entry : list) {
            if (done) return;
            // add another entry to aggregate?
            if (aggregateSize + entry.getSize() < maxSizeUncompressed) {
                if (aggregate.size() == 0) {
                    //
                    //   add first entry to pending agg
                    //
                    firstEntryDate = new Date(entry.getTimestamp());
                    firstEntryYYYYMMDD = sdfArchivePath.format(firstEntryDate);
                    if (firstEntryYYYYMMDD.equals(nowYYYYMMDD)) {
                        return; // dont process logsets from today
                    }
                    aggregate.add(entry);
                    aggregateSize += entry.getSize();
                    lastEntryDate = firstEntryDate;
                } else if (firstEntryYYYYMMDD.equals(sdfArchivePath.format(new Date(entry.getTimestamp())))) { // same day
                    //
                    //  add entry to pending agg
                    //
                    lastEntryDate = new Date(entry.getTimestamp());
                    aggregate.add(entry);
                    aggregateSize += entry.getSize();
                } else {
                    // entry is for another day, process pending agg
                    emitAgg = true;
                }

            } else { // entry wont fit, so process the pending aggregate
                emitAgg = true;
            }
            if (emitAgg) {
                emitAgg = false;
                processAggregate(aggregate, firstEntryDate, lastEntryDate, aggregateSize);
                // set current entry to be initial in next aggregate
                aggregate.clear();
                firstEntryDate = new Date(entry.getTimestamp());
                firstEntryYYYYMMDD = sdfArchivePath.format(firstEntryDate);
                if (firstEntryYYYYMMDD.equals(nowYYYYMMDD)) {
                    continue; // dont process logsets from today
                }
                lastEntryDate = firstEntryDate;
                aggregate.add(entry);
                aggregateSize = entry.getSize();

                try { // leave a little bit of slack for others
                    Thread.sleep(sleepPeriodMsec/100L + 1000L);
                } catch (InterruptedException e) {
                    if (Thread.interrupted()) {
                        done = true;
                        return;
                    }
                }
            }
        }
        if (aggregateSize > 0) {
            processAggregate(aggregate, firstEntryDate, lastEntryDate, aggregateSize);
        }
    }

    /**
     * Process an aggregate of logsets by archiving them together.
     * @param aggregate a list of logsets to compress in aggregate.
     * @param firstEntryDate
     * @param lastEntryDate
     * @param estimatedSpaceBytes total size of aggregate.
     */
    private void processAggregate(ArrayList<LogSetEntry> aggregate, Date firstEntryDate, Date lastEntryDate, long estimatedSpaceBytes) {
        long tStart = System.currentTimeMillis();
        long tCopyToLocal;
        long tCompress;
        long tCopyFromLocal;
        final String TAR_FILE_EXT = ".tar.xz";

        // check if it is time to stop
        if (limitReached()) return;

        final String aggName = sdfArchive.format(firstEntryDate) + "-" + sdfArchive.format(lastEntryDate);

        final File localDir = new File(localTemp, aggName);
        if (dryRun) {
            emsg("dryRun: make local dir: " + localDir.getAbsolutePath());
        } else {
            localDir.mkdir();
        }

        // check available disk space under localTempParent
        long spaceAvailable = localTempParent.getUsableSpace();
        if (estimatedSpaceBytes + (estimatedSpaceBytes/20L) > spaceAvailable) {
            emsg("ERROR:  Insufficient space under " + localTempParent.getAbsolutePath());
            return; // giving up early
        }

        final String compressedTarFileHdfs = aggName + TAR_FILE_EXT;
        final String hdfsBaseDest = hdfsArchivePath + "/" + sdfArchivePath.format(firstEntryDate);
        final String hdfsDest = hdfsBaseDest + "/" + compressedTarFileHdfs;

        //     check if already archived
        if (hdfsExists(hdfsDest)) {
            rmrLocal(localDir); //cleanup
            // already archived (we assume)
            if (noHdfsDelete || dryRun) {
                emsg("INFO: already archived, you might need to remove from hdfs manually: ");
                for (LogSetEntry entry : aggregate) {
                    emsg("    " + entry.getHdfsPath());
                }
            } else {
                emsg("INFO: already archived, removing these from hdfs now: ");
                for (LogSetEntry entry : aggregate) {
                    emsg("    " + entry.getHdfsPath());
                }
                deleteFromHdfs(aggregate);
            }
            return;
        }

        // copy aggregate from hdfs to local fs
        boolean cpLocalOkay;
        for (LogSetEntry entry : aggregate) {
            if (limitReached()) {
                done = true;
                rmrLocal(localDir);
                return;
            }
            cpLocalOkay = copyToLocal(entry.getHdfsPath(), localDir.getAbsolutePath());
            if (! cpLocalOkay) {
                emsg("ERROR:  copyToLocal failed, src=" + entry.getHdfsPath() + "  dest=" + localDir.getAbsolutePath());
                // cleanup
                rmrLocal(localDir);
                return; // giving up early
            }
        }
        tCopyToLocal = System.currentTimeMillis();

        // check if it is time to stop
        if (limitReached()) {
            // cleanup
            rmrLocal(localDir);
            return;
        }

        // compress local
        String compressedTarFileLocal;
        String compressError = null;
        try {
            compressedTarFileLocal = localTarCompress(localDir, "agg" + System.nanoTime() + TAR_FILE_EXT);

        } catch (IOException e) {
            compressError = e.toString();
            compressedTarFileLocal = null;
        }
        if (compressedTarFileLocal == null) {
            if (!done) {
                emsg("UnexpectedException: localTarCompress: " + compressError);
            }
            // cleanup after failure
            if (!noLocalDelete) {
                rmrLocal(localDir);
            }
            return;
        }
        File tFile = new File(compressedTarFileLocal);
        if (! dryRun  && ! tFile.exists()) {
            emsg("ERROR: Missing tFile=" + tFile.getAbsolutePath());
            return;
        }
        long compressedSize = tFile.length();
        tCompress = System.currentTimeMillis();

        // check if it is time to stop
        if (limitReached()) {
            // cleanup
            rmrLocal(localDir);
            tFile.delete();
            return;
        }

        // write local compressed tar to hdfs
        makeDirs(hdfsBaseDest);
        boolean okay = copyFromLocal(tFile.getAbsolutePath(), hdfsDest);
        tCopyFromLocal = System.currentTimeMillis();
        if (okay) {
            // remove locally cached files
            if (!noLocalDelete) {
                rmrLocal(localDir);
                tFile.delete();
            }
            // remove from hdfs the logsets that are archived
            if (!noHdfsDelete) {
                deleteFromHdfs(aggregate);
            }
            // report on work done
            long sizeDisplayed = estimatedSpaceBytes/(1024L*1024L*1024L);
            String units = "GB";
            if (sizeDisplayed == 0L) {
                sizeDisplayed = estimatedSpaceBytes/(1024L*1024L);
                units = "MB";
            }
            if (sizeDisplayed == 0) {
                sizeDisplayed = estimatedSpaceBytes/1024L;
                units = "KB";
            }
            long tNow = System.currentTimeMillis();
            long timeOverallMsec = tNow - tStart;
            long timeCopyToLocalMsec = tCopyToLocal - tStart;
            long timeCompressMsec = tCompress - tCopyToLocal;
            long timeCopyFromLocalMsec = tCopyFromLocal - tCompress;
            long timeCleanup = tNow - tCopyFromLocal;
            String timingMsg = "(Overall=" + (timeOverallMsec/1000.) +
                                " CopyToLocal=" + (timeCopyToLocalMsec/1000.) +
                                 " Compress=" + (timeCompressMsec/1000.) +
                                 " CopyFromLocal=" + (timeCopyFromLocalMsec/1000.) +
                                 " Cleanup=" + (timeCleanup/1000.) +
                                 " compressRatio=" + (((float)estimatedSpaceBytes)/((float)compressedSize)) +
                                 " overallMBperSec=" + (((estimatedSpaceBytes/1024)/((tNow - tStart)/1000L))/1024.)  +
                                 ") ";
            emsg("INFO: archived, " + timingMsg + sizeDisplayed + " " + units + " to " + hdfsBaseDest);
        } else {
            emsg("ERROR: could not copyFromLocal to HDFS " + hdfsDest);
        }
    }

    private void deleteFromHdfs(ArrayList<LogSetEntry> aggregate) {
        for (LogSetEntry entry : aggregate) {
            // remove old selected entries from hdfs: /analyzed/
            boolean rmOkay = rmr(entry.getHdfsPath());
            if (!rmOkay) {
                emsg("ERROR: unable to remove HDFS " + entry.getHdfsPath());
            }
        }
    }

    private void rmrLocal(File file) {
        if (! file.exists()) {
            return;
        }
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                rmrLocal(f);
            }
        }
        file.delete();
    }

    private boolean rmr(String hdfsPath) {
        // NOT using "-skipTrash" because do not have permission to delete.
        //  without "-skipTrash", it will move to /user/unravel/.Trash/Current
        //  and the trash-remover will operate on unowned files.
        String[] cmd1 = { "-rmr",  hdfsPath };
        String[] cmd2 = { "-rm", "-R", hdfsPath };
        String msg = "hadoop fs " + cmd1[0] + " " + cmd1[1];
        if (dryRun) {
            emsg("dryRun: " + msg);
        } else {
            BufferedReader in = doFsShell(cmd1);
            if (in == null) { // try the newer arg format
                in = doFsShell(cmd2);
            }
            if (in == null) {
                emsg("WARN: non-zero ret code from:  " + msg);
                return false;
            }
        }
        return true;
    }

    private boolean copyToLocal(String srcPath, String localDest) {
        String[] cmd = { "-copyToLocal", srcPath, localDest };
        String msg = "hadoop fs " + cmd[0] + " " + cmd[1] + " "+ cmd[2];
        if (dryRun) {
            emsg("dryRun: " + msg);
        } else {
            BufferedReader in = doFsShell(cmd);
            if (in == null) {
                emsg("WARN: non-zero ret code from:  " + msg);
                return false;
            }
        }
        return true;
    }


    private boolean copyFromLocal(String localSrc, String hdfsDest) {
        String[] cmd = { "-copyFromLocal", localSrc, hdfsDest };
        String msg = "hadoop fs " + cmd[0] + " " + cmd[1] + " "+ cmd[2];
        if (dryRun) {
            emsg("dryRun: " + msg);
        } else {
            BufferedReader in = doFsShell(cmd);
            if (in == null) {
                emsg("WARN: non-zero ret code from:  " + msg);
                return false;
            }
        }
        return true;
    }

    private boolean makeDirs(String hdfsDir) {
        String[] cmd = { "-mkdir", hdfsDir };
        String msg = "hadoop fs " + cmd[0] + " " + cmd[1];
        if (dryRun) {
            emsg("dryRun: " + msg);
        } else {
            BufferedReader in = doFsShell(cmd);
            if (in == null) {
                emsg("NO_WORRIES: non-zero ret code from:  " + msg);
                return false;
            }
        }
        return true;
    }

    private boolean hdfsExists(String hdfsPath) {
        String[] cmd = { "-stat", hdfsPath };
        String msg = "hadoop fs " + cmd[0] + " " + cmd[1];
        BufferedReader in = doFsShell(cmd);
        if (in == null) { // does not exist
            return false;
        }
        return true;
    }

    /**
     *
     * @param inputDir
     * @return path to resultant compressed file, or null if failed.
     * @throws IOException
     */
    private String localTarCompress(File inputDir, String compressedTarFileBasename) throws IOException {

        final String outFile = inputDir.getParentFile().getAbsolutePath() + "/" + compressedTarFileBasename;
        final String inFile = inputDir.getName();
        final File wd = inputDir.getParentFile();
        if (dryRun) {
            emsg("dryRun: cd " + wd.getAbsolutePath() + "; tar cJf " + outFile + " " + inFile);
            final File tarFile = new File(outFile);
            tarFile.createNewFile(); // fake
        } else {
            final ProcessBuilder pb = new ProcessBuilder("tar", "cJf", outFile, inFile);
            pb.directory(wd);
            Process p = pb.start();
            try {
                p.waitFor();
                if (p.exitValue() != 0) {
                    return null;
                }
            } catch (InterruptedException e) {
                if (Thread.interrupted()) {
                    done = true;
                    return null;
                }
            }
        }
        // verify result file exists
        final File oFile = new File(outFile);
        if (! oFile.exists()) return null;
        return outFile;
    }

    /**
     * Delete archived files that contain information older than the retention limit.
     * @param targetDir
     */
    private void cleanupDir(String targetDir) {
        //ToDo: implement cleanupDir()
    }

    private Configuration getConf() {
        Configuration conf = new Configuration();
        conf.addResource(new Path(hadoopConfPath + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfPath + "/hdfs-site.xml"));
        return conf;
    }

    private long parseToLong(String s) {
        long result = 0L;
        try {
            result = Long.parseLong(s);
        } catch (NumberFormatException nfe) {
            System.err.println("parse error on:  " + s);
            System.exit(3);
        }
        return result;
    }

    private long parseToMSec(String s) {
        String s_stripped = s;
        long multiplier = 1000L;
        if (s.endsWith("s")) {
            s_stripped = s.substring(0, s.length()-1);
        } else if (s.endsWith("m")) {
            multiplier = multiplier * 60L;
            s_stripped = s.substring(0, s.length()-1);
        } else if (s.endsWith("h")) {
            multiplier = multiplier * 60L * 60L;
            s_stripped = s.substring(0, s.length()-1);
        } else if (s.endsWith("d")) {
            multiplier = multiplier * 60L * 60L * 24L;
            s_stripped = s.substring(0, s.length()-1);
        }
        long result = multiplier * parseToLong(s_stripped);
        return result;
    }

    private long parseToBytes(String s) {
        String s_stripped = s;
        long multiplier = 1024L * 1024L;
        if (s.endsWith("m")) {
            s_stripped = s.substring(0, s.length()-1);
        } else if (s.endsWith("g")) {
            multiplier = multiplier * 1024L;
            s_stripped = s.substring(0, s.length()-1);
        } else if (s.endsWith("t")) {
            multiplier = multiplier * 1024L * 1024L;
            s_stripped = s.substring(0, s.length()-1);
        }
        long result = multiplier * parseToLong(s_stripped);
        return result;
    }

    /**
     * Perform hadoop fs -du on the given path.
     * Directories contained by the given path are expected to be in Hive query name format.
     * @param path to hdfs.
     * @return list of LogSetEntry, 0 length if nothing found.
     */
    private List<LogSetEntry> hdfsDUdir(String path) {
        String[] cmd = { "-du", path };
        BufferedReader in = doFsShell(cmd);
        List<LogSetEntry> results = new ArrayList<LogSetEntry>();

        try {

            String currLine = in.readLine();
            while (currLine != null) {
                if (! currLine.startsWith("Found ")) {
                    // process entry
                    //   example du entry:
                    // 1139 hdfs://10.12.51.88:9000/user/unravel/analyzed/someone_20131203141111_d9e5ae49-41b1-4949-a2f3-c08ca3b83bb7
                    //     OR:
                    // 9999 hdfs://w.x.y.z:port/$BASE_DIR/${NAME}_YYYYMMDDHH*
                    //  Note that hdfs://ip:port might not appear in the -du output
                    //
                    Matcher m = archivePattern.matcher(currLine);
                    if (m.matches()) {
                        String sizeStr = m.group(1);
                        String ipaddrPort = m.group(3);
                        String dirName = m.group(4);
                        Matcher m2 = dirNamePattern.matcher(dirName);
                        if (m2.matches()) {
                            String userName = m2.group(1);
                            String yyyymmddhhmm = m2.group(2);
                            Date tdate;
                            try {
                                tdate = sdfFileQueryIdTimestamp.parse(yyyymmddhhmm);
                            } catch (ParseException e) {
                                emsg("ERROR: Could not parse yyyymmddhhmm: " + yyyymmddhhmm);
                                tdate = new Date();
                            }
                            String hdfsPath = path + "/" + dirName;
                            LogSetEntry entry =
                              new LogSetEntry(dirName, hdfsPath, parseToLong(sizeStr), tdate.getTime(), userName);
                            results.add(entry);
                        } else {
                            emsg("ERROR: matcher2 failed on: " + dirName);
                        }
                    } else {
                        emsg("ERROR: matcher1 failed on: " + currLine);
                    }
                }
                currLine = in.readLine();
            }
        } catch (IOException not_happening) { }
        return results;
    }

    /**
     * Wrapper for FsShell.
     * We use FsShell in command line form, but directly access the object to avoid exec().
     * We could override the package level permissions by subclassing in the same package,
     * but the command line form is best for compatibility.
     * @param argv
     * @return BufferedReader of stdout from command, or null if non-zero ret code.
     */
    private BufferedReader doFsShell(String[] argv) {
        FsShell shell = new FsShell(getConf());
        int res;
        try {
            baos_out.reset();
            res = ToolRunner.run(shell, argv);
            if (res != 0) {
                return null;
            }
        } catch (Exception ignored) {
            return null;
        } finally {
            try {
                ;//TESTING EMR has no .close() here  //  shell.close(); // v1.1.2.x has close()
            } catch (Exception ignored) {
            }
        }
        // extract stdout
        BufferedReader in
          = new BufferedReader(new InputStreamReader( new ByteArrayInputStream(baos_out.toByteArray())) );
        return in;
    }

}