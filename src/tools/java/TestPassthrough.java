import java.io.BufferedInputStream;

import java.io.FileInputStream;
import com.netflix.hollow.index.HollowPrimaryKeyIndex;
import java.io.File;
import com.netflix.hollow.util.HollowRecordStringifier;
import java.io.ByteArrayInputStream;
import com.netflix.hollow.read.engine.HollowBlobReader;
import com.netflix.hollow.read.engine.HollowReadStateEngine;
import com.netflix.hollow.util.HollowWriteStateCreator;
import java.io.ByteArrayOutputStream;
import com.netflix.hollow.write.HollowBlobWriter;
import com.netflix.hollow.write.HollowWriteStateEngine;
import com.netflix.hollow.jsonadapter.HollowJsonAdapterStateEnginePopulator;
import com.netflix.hollow.HollowSchema;
import com.netflix.hollow.util.HollowSchemaParser;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import org.junit.Test;


public class TestPassthrough {


    @Test
    public void test() throws Exception {
        Collection<HollowSchema> schemas = HollowSchemaParser.parseCollectionOfSchemas(loadFile("/common/git/videometadata-converter/src/main/resources/schemas.txt"));

        HollowWriteStateEngine stateEngine = HollowWriteStateCreator.createWithSchemas(schemas);

        HollowJsonAdapterStateEnginePopulator populator = new HollowJsonAdapterStateEnginePopulator(stateEngine, "VideoType");
        //populator.addPassthroughDecoratedType("CharacterArtworkAttributes");

        //String characterArtworkRecord = loadFile("/space/temp/CharacterArtwork.txt");
        //populator.processRecord(characterArtworkRecord);
        populator.populate(new File("/space/coldstarts/VideoType.json_V1"));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        HollowBlobWriter writer = new HollowBlobWriter(stateEngine);
        writer.writeSnapshot(baos);

        HollowReadStateEngine readEngine = new HollowReadStateEngine();
        HollowBlobReader reader = new HollowBlobReader(readEngine);
        reader.readSnapshot(new ByteArrayInputStream(baos.toByteArray()));

        HollowPrimaryKeyIndex idx = new HollowPrimaryKeyIndex(readEngine, "VideoType", "videoId");
        int ordinal = idx.getMatchingOrdinal((long)70305056);

        HollowRecordStringifier stringifier = new HollowRecordStringifier();
        System.out.println(stringifier.stringify(readEngine, "VideoType", ordinal));
    }

    @Test
    public void test2() throws Exception {
        HollowReadStateEngine readEngine = new HollowReadStateEngine();
        HollowBlobReader reader = new HollowBlobReader(readEngine);
        reader.readSnapshot(new BufferedInputStream(new FileInputStream("/space/local-blob-store/vmsinput-snapshot-20160218221244741")));

        HollowPrimaryKeyIndex idx = new HollowPrimaryKeyIndex(readEngine, "VideoType", "videoId");
        int ordinal = idx.getMatchingOrdinal((long)70305056);

        HollowRecordStringifier stringifier = new HollowRecordStringifier();
        System.out.println(stringifier.stringify(readEngine, "VideoType", ordinal));
    }

    private String loadFile(String filename) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filename));

        StringBuilder builder = new StringBuilder();

        String line = reader.readLine();
        while(line != null) {
            builder.append(line + "\n");
            line = reader.readLine();
        }

        reader.close();
        return builder.toString();
    }
}
