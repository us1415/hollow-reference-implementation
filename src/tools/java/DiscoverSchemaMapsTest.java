import com.netflix.hollow.jsonadapter.HollowDiscoveredSchema;
import com.netflix.hollow.jsonadapter.HollowJsonAdapterSchemaDiscoverer;

import java.io.File;
import java.util.Collection;

import org.junit.Test;


public class DiscoverSchemaMapsTest {

    @Test
    public void test() throws Exception {
        Collection<HollowDiscoveredSchema> schemas = discoverSchemas("TurboCollections",
                "TurboCollectionsDnTranslatedTexts", "TurboCollectionsSnTranslatedTexts", "TurboCollectionsKc.cnTranslatedTexts",
                "TurboCollectionsTdnTranslatedTexts", "TurboCollectionsKag.knTranslatedTexts", "TurboCollectionsNav.snTranslatedTexts");

        for(HollowDiscoveredSchema schema : schemas) {
            System.out.println(schema.toHollowSchema());
        }
    }

    private Collection<HollowDiscoveredSchema> discoverSchemas(String type, String... mapTypes) throws Exception {
        HollowJsonAdapterSchemaDiscoverer discoverer = new HollowJsonAdapterSchemaDiscoverer(type);
        discoverer.addMapTypes(mapTypes);

        File jsonFile = new File("/space/coldstarts/" + type + ".json_V1");
        if(!jsonFile.exists())
            jsonFile = new File("/space/coldstarts/" + type + ".json_v1");

        Collection<HollowDiscoveredSchema> schemas = discoverer.discoverSchemas(jsonFile, 1000);

        HollowJsonAdapterSchemaDiscoverer.analyzeSchemas(schemas, 10);

        return schemas;
    }


}
