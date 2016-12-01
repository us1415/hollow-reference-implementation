


import com.netflix.hollow.diff.HollowDiff;
import com.netflix.hollow.diff.HollowTypeDiff;
import com.netflix.hollow.diff.count.HollowFieldDiff;
import com.netflix.hollow.jsonadapter.ColdstartJsonAdapter;
import com.netflix.hollow.read.engine.HollowBlobReader;
import com.netflix.hollow.read.engine.HollowReadStateEngine;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Test;

public class ConvertAndDiffTest {
    private static String getFileName(String fullFilePath) {
        int start = fullFilePath.contains("/") ? (fullFilePath.lastIndexOf("/") + 1) : 0;
        int end = fullFilePath.contains(".") ? fullFilePath.lastIndexOf(".") : fullFilePath.length();
        return fullFilePath.substring(start, end);
    }

    private static class ConvertJob implements Runnable {
        private final String jsonFile;
        private final String hollowFile;
        private final String[] mapTypes;

        public ConvertJob(String jsonFile, String hollowFile, String[] mapTypes) {
            this.jsonFile = jsonFile;
            this.hollowFile = hollowFile;
            this.mapTypes = mapTypes;
        }

        @Override
        public void run() {
            try {
                System.out.println("Processing: " + jsonFile);
                long s = System.currentTimeMillis();

                String typeName = getFileName(jsonFile);
                ColdstartJsonAdapter adapter = new ColdstartJsonAdapter();
                if (mapTypes != null) adapter.addMapTypes(mapTypes);
                adapter.convertToHollowBlob(new File(jsonFile), typeName);
                adapter.writeHollowSnapshot(new File(hollowFile));

                System.out.println("Created: " + hollowFile + "\tduration=" + (System.currentTimeMillis() - s) + "ms");
            } catch (Exception ex) {
                System.err.println("Failed to convert=" + jsonFile);
                ex.printStackTrace();
            }
        }

    }


    public static HollowReadStateEngine load(String filename) throws IOException {
        HollowReadStateEngine stateEngine = new HollowReadStateEngine(true);
        HollowBlobReader reader = new HollowBlobReader(stateEngine);
        reader.readSnapshot(new BufferedInputStream(new FileInputStream(filename)));
        return stateEngine;
    }

    private static void addTypeDiff(HollowDiff diff, String type, String... matchPaths) {
        HollowTypeDiff typeDiff = diff.addTypeDiff(type);
        for (String path : matchPaths) {
            typeDiff.addMatchPath(path);
        }
    }

    private static void runJobs(Runnable... jobs) throws InterruptedException, ExecutionException {
        int jobSize = jobs.length;
        List<Future<?>> futures = new ArrayList<>(jobSize);
        ExecutorService service = Executors.newFixedThreadPool(jobSize);

        for (Runnable job : jobs) {
            futures.add(service.submit(job));
        }

        for (Future<?> f : futures) {
            f.get();
        }
        service.shutdown();
    }

    @Test
    public void convertAndDiff() throws Exception {
        long start = System.currentTimeMillis();
        StringBuilder summarySB = new StringBuilder();
        String fromHolow = "/tmp/VideoRights.hollow_from";
        String toHollow = "/tmp/VideoRights.hollow_to";
        String[] mapTypes = new String[]{"VideoRights.flags.firstDisplayDates"};

        // Convert json to hollow
        long s = System.currentTimeMillis();
        ConvertJob job1 = new ConvertJob("/tmp/sample/VideoRights.json_2015_09_21", fromHolow, mapTypes);
        ConvertJob job2 = new ConvertJob("/tmp/sample/VideoRights.json_2015_09_22", toHollow, mapTypes);
        runJobs(job1, job2);
        summarySB.append("ConvertJob duration=" + (System.currentTimeMillis() - s)).append("\n");

        // Load from and to Hollow Blobs
        s = System.currentTimeMillis();
        HollowReadStateEngine from = load(fromHolow);
        HollowReadStateEngine to = load(toHollow);
        summarySB.append("Load Hollow duration=" + (System.currentTimeMillis() - s)).append("\n");

        // Compute diffs from two hollow blobs
        s = System.currentTimeMillis();
        HollowDiff diff = new HollowDiff(from, to);
        addTypeDiff(diff, "VideoRights", "movieId", "countryCode.value");
        diff.calculateDiffs();
        summarySB.append("Diff Hollow duration=" + (System.currentTimeMillis() - s)).append("\n");

        // Print out diffs
        System.out.println("\n\nDIFF SUMMARY:");
        for (HollowTypeDiff typeDiff : diff.getTypeDiffs()) {
            System.out.println("Type:" + typeDiff.getTypeName());
            System.out.println("Total number of matched records for type: " + typeDiff.getTotalNumberOfMatches());
            System.out.println("Total number of removed records: " + typeDiff.getUnmatchedOrdinalsInFrom().size());
            System.out.println("Total number of added records: " + typeDiff.getUnmatchedOrdinalsInTo().size());

            for (HollowFieldDiff fieldDiff : typeDiff.getFieldDiffs()) {
                System.out.println("\t" + fieldDiff.getFieldIdentifier().toString());
                System.out.println("\t\t Total objects with any diffs for selected field: " + fieldDiff.getNumDiffs());
                System.out.println("\t\t Total diff score for selected field: " + fieldDiff.getTotalDiffScore());

                System.out.println();
            }
        }

        summarySB.append("Total Duration=" + (System.currentTimeMillis() - start)).append("\n");
        System.out.println(summarySB);
    }
}
