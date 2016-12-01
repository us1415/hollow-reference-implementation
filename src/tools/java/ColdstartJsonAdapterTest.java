


import com.netflix.hollow.diff.HollowDiff;
import com.netflix.hollow.diff.HollowTypeDiff;
import com.netflix.hollow.diff.count.HollowFieldDiff;
import com.netflix.hollow.jsonadapter.AbstractHollowJsonAdaptorTask;
import com.netflix.hollow.jsonadapter.ColdstartJsonAdapter;
import com.netflix.hollow.jsonadapter.HollowDiscoveredSchema;
import com.netflix.hollow.jsonadapter.HollowJsonAdapterSchemaDiscoverer;
import com.netflix.hollow.jsonadapter.field.impl.DoubleFieldProcessor;
import com.netflix.hollow.jsonadapter.field.impl.HashHexFieldProcessor;
import com.netflix.hollow.jsonadapter.field.impl.RoundingNumericFieldProcessor;
import com.netflix.hollow.objects.HollowObject;
import com.netflix.hollow.objects.generic.GenericHollowRecordHelper;
import com.netflix.hollow.read.engine.HollowBlobReader;
import com.netflix.hollow.read.engine.HollowReadStateEngine;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.zip.ZipInputStream;
import org.junit.Test;

public class ColdstartJsonAdapterTest {
    public static final String MUTATION_TYPE = "beehive";
    public static final String INPUT_COLDSTART_FILE = "/space/coldstarts/" + MUTATION_TYPE + ".zip";
    public static final String EXPANDED_COLDSTART_FOLDER = "/space/coldstarts/jsons";
    public static final String OUTPUT_HOLLOW_FILE = "/tmp/" + MUTATION_TYPE + ".hollow.2";

    @Test
    public void debugSchema() throws Exception {
        AbstractHollowJsonAdaptorTask.isDebug = false;

        String testFile = "/tmp/R2.json";
        String realFile = "/space/coldstarts/jsons/beehive.zip/Rollout.json_V1";

        HollowJsonAdapterSchemaDiscoverer discoverer = new HollowJsonAdapterSchemaDiscoverer("Rollout");
        discoverer.addMapTypes("Rollout.phases.windows");
        Collection<HollowDiscoveredSchema> schemas = discoverer.discoverSchemas(new File(realFile), null);

        HollowJsonAdapterSchemaDiscoverer.analyzeSchemas(schemas, 10);
    }


    @Test
    public void convertColdstartToHollowBlob() throws Exception {
        boolean isWriteHollowToDisk = true;
        boolean isReuseUnzippedJsonFiles = false;
        File inputColdstartFile = new File(INPUT_COLDSTART_FILE);
        File outputHollowFile = new File(OUTPUT_HOLLOW_FILE);
        File expandedColdStartFolder = new File(EXPANDED_COLDSTART_FOLDER, inputColdstartFile.getName());

        ColdstartJsonAdapter adapter = isReuseUnzippedJsonFiles ? new ColdstartJsonAdapter(expandedColdStartFolder) : new ColdstartJsonAdapter();
        //adapter.setSchemaMaxSampling(1024);
        adapter.setSchemaMaxSampling(10000);
        adapter.setFieldProcessors(new HashHexFieldProcessor("PackagesDownloadables", "sha1HashHex", 40),
                new HashHexFieldProcessor("PackagesDownloadables", "crc32cHashHex", 8),
                new RoundingNumericFieldProcessor("PackagesDownloadables", "createdTimeMillies", 1000),
                new DoubleFieldProcessor("PackagesDownloadables", "fps")
                );
        adapter.addMapTypes(
                "Rollout.phases.windows",
                "Rollout.phases.elements.trailers.supplementalInfo",
                "Rollout.phases.elements.localized_metadata",
                "Rollout.launchDates",
                "LocalizedCharacter.translatedTexts",
                "LocalizedMetadata.translatedTexts");

        //        adapter.setMapTypes("Rollout.phases.windows");

        long startTime = System.currentTimeMillis();
        if (isReuseUnzippedJsonFiles && expandedColdStartFolder.exists()) {
            adapter.convertToHollowBlobSkippingFiles("Beacon.json_v1");
        } else {
            adapter.convertToHollowBlob(new ZipInputStream(new FileInputStream(inputColdstartFile)), "Beacon.json_v1");
        }

        if (isWriteHollowToDisk) adapter.writeHollowSnapshot(outputHollowFile);
        if (!isReuseUnzippedJsonFiles) adapter.cleanUp();
        System.out.println((System.currentTimeMillis() - startTime) + " ms");
    }

    /*
    @Test
    public void pullDownLatest() throws Exception {
        VMSTestPlatformInitializer.initLibraries();

        FileStore fileStore = VMSInjectionManager.get().getFileStore();

        S3Object publishedFile = fileStore.getPublishedFile("com.netflix.beehive.metadata.vms.json");

        fileStore.copyFile(publishedFile, new File("/space/coldstarts/beehive-coldstart2.zip"));

    }
     */


    /*
    @Test
    public void diffUI() throws Exception {

        HollowReadStateEngine from = load("/tmp/beehive-coldstart.hollow");
        HollowReadStateEngine to = load("/tmp/beehive-coldstart2.hollow");

        HollowDiff diff = beehiveDiff(from, to);

        HollowDiffUIServer server = new HollowDiffUIServer(7777);

        server.addDiff("diff", diff);

        server.start();
        server.join();
    }
     */

    @Test
    public void programmaticallyRetrieveChanges() throws Exception {
        HollowReadStateEngine from = load("/tmp/beehive-coldstart.hollow");
        HollowReadStateEngine to = load("/tmp/beehive-coldstart2.hollow");

        HollowDiff diff = beehiveDiff(from, to);

        HollowTypeDiff typeDiff = getTypeDiff(diff, "VideoType");                                    /// specify a top-level type as the name of the json file
        HollowFieldDiff fieldDiff = getFieldDiff(diff, "VideoType.type.element.isContentApproved");  /// specify a field like this

        System.out.println("Total objects with any diffs for selected field: " + fieldDiff.getNumDiffs());
        System.out.println("Total diff score for selected field: " + fieldDiff.getTotalDiffScore());
        System.out.println("Total number of matched records for type: " + typeDiff.getTotalNumberOfMatches());
        System.out.println("Total number of removed records: " + typeDiff.getUnmatchedOrdinalsInFrom().size());
        System.out.println("Total number of added records: " + typeDiff.getUnmatchedOrdinalsInTo().size());


        //// print out all of the ids which were different for the field isContentApproved in VideoType
        for(int i=0;i<fieldDiff.getNumDiffs();i++) {
            int fromOrdinal = fieldDiff.getFromOrdinal(i);
            HollowObject obj = (HollowObject)GenericHollowRecordHelper.instantiate(diff.getFromStateEngine(), "VideoType", fromOrdinal);
            System.out.println("Diff ID: " + obj.getInt("videoId"));
        }
    }

    private HollowFieldDiff getFieldDiff(HollowDiff diff, String fieldPath) {
        String typeName = fieldPath.substring(0, fieldPath.indexOf('.'));
        HollowTypeDiff typeDiff = getTypeDiff(diff, typeName);
        if(typeDiff != null) {
            for(HollowFieldDiff fieldDiff : typeDiff.getFieldDiffs()) {
                if(fieldDiff.getFieldIdentifier().toString().startsWith(fieldPath + " "))
                    return fieldDiff;
            }
        }
        return null;
    }

    private HollowTypeDiff getTypeDiff(HollowDiff diff, String typeName) {
        for(HollowTypeDiff typeDiff : diff.getTypeDiffs())
            if(typeDiff.getTypeName().equals(typeName))
                return typeDiff;
        return null;
    }


    private HollowDiff beehiveDiff(HollowReadStateEngine from, HollowReadStateEngine to) {
        HollowDiff diff = new HollowDiff(from, to);

        addTypeDiff(diff, "Bcp47Code", "bcp47Code.value");
        addTypeDiff(diff, "VideoGeneral", "videoId");
        addTypeDiff(diff, "VideoType", "videoId");
        addTypeDiff(diff, "VideoDisplaySet", "topNodeId");
        addTypeDiff(diff, "Rollout", "movieId", "rolloutId");
        addTypeDiff(diff, "VideoAward", "videoId");
        addTypeDiff(diff, "VideoRating", "videoId");
        addTypeDiff(diff, "CertificationSystem", "certificationSystemId");
        addTypeDiff(diff, "Character", "characterId");
        addTypeDiff(diff, "LocalizedCharacter", "characterId");
        addTypeDiff(diff, "LocalizedMetadata", "movieId");
        addTypeDiff(diff, "Trailer", "movieId");
        addTypeDiff(diff, "VideoDate", "videoId");
        addTypeDiff(diff, "VideoPerson", "personId");
        addTypeDiff(diff, "VMSAward", "awardId");

        diff.calculateDiffs();

        return diff;
    }

    private void addTypeDiff(HollowDiff diff, String type, String... matchPaths) {
        HollowTypeDiff typeDiff = diff.addTypeDiff(type);
        for(String path : matchPaths) {
            typeDiff.addMatchPath(path);
        }
    }

    private HollowReadStateEngine load(String filename) throws IOException {
        HollowReadStateEngine stateEngine = new HollowReadStateEngine(true);

        HollowBlobReader reader = new HollowBlobReader(stateEngine);
        reader.readSnapshot(new BufferedInputStream(new FileInputStream(filename)));

        return stateEngine;
    }



    /**
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.RIGHTS, "com.netflix.beehive.rights.vms.json");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.BEEHIVE, "com.netflix.beehive.metadata.vms.json");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.MPL, "com.netflix.mpl.test.coldstart.services");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.L10N, "com.netflix.beehive.asterix.vms.json");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.ARTWORK, "com.netflix.cme.artwork.json");
    //COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.ROLLOUT, "com.netflix.beehive.oscar.vms.json");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.KIPPER, "com.netflix.beehive.kipper.vms.json");
    COLDSTART_KEYBASE_DEFAULTS.put(MutationGroup.TOPN, "com.netflix.vms.input.topn");
     * @throws IOException
     **/


}
