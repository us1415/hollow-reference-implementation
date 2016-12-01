import com.netflix.hollow.util.HollowWriteStateCreator;

import com.netflix.hollow.HollowSchema;
import com.netflix.hollow.jsonadapter.HollowDiscoveredSchema;
import com.netflix.hollow.jsonadapter.HollowJsonAdapter;
import com.netflix.hollow.jsonadapter.HollowJsonAdapterSchemaDiscoverer;
import com.netflix.hollow.jsonadapter.HollowJsonAdapterStateEnginePopulator;
import com.netflix.hollow.jsonadapter.field.impl.DoubleFieldProcessor;
import com.netflix.hollow.jsonadapter.field.impl.HashHexFieldProcessor;
import com.netflix.hollow.jsonadapter.field.impl.RoundingNumericFieldProcessor;
import com.netflix.hollow.util.HollowSchemaParser;
import com.netflix.hollow.write.HollowBlobWriter;
import com.netflix.hollow.write.HollowWriteStateEngine;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import org.apache.commons.io.IOUtils;
import org.junit.Test;


public class PopulateStateEnginesTest {

    @Test
    public void producePackages() throws Exception {
        HollowWriteStateEngine stateEngine = new HollowWriteStateEngine();

        HollowJsonAdapter adapter = new HollowJsonAdapter(stateEngine, "Packages");
        adapter.addFieldProcessors(new HashHexFieldProcessor("PackagesDownloadables", "sha1HashHex", 40),
                new HashHexFieldProcessor("PackagesDownloadables", "crc32cHashHex", 8),
                new RoundingNumericFieldProcessor("PackagesDownloadables", "createdTimeMillies", 1000),
                new DoubleFieldProcessor("PackagesDownloadables", "fps")
        );

        addJsonData(stateEngine, "Packages");

        HollowBlobWriter writer = new HollowBlobWriter(stateEngine);
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream("/tmp/VMSInputPackagesData.hollow"));
        writer.writeSnapshot(os);
        os.close();
    }

    @Test
    public void produceEverythingElse() throws Exception {
        HollowWriteStateEngine stateEngine = new HollowWriteStateEngine();
        HollowWriteStateCreator.populateStateEngineWithTypeWriteStates(stateEngine, loadSchemas());
        //writer = new PrintWriter("/space/hollowinput/schemas.txt");

        //addJsonData(stateEngine, "TurboCollections");
        addJsonData(stateEngine, "AltGenres", "AltGenresShortNameTranslatedTexts", "AltGenresAlternateNamesTranslatedTexts", "AltGenresDisplayNameTranslatedTexts");
        addJsonData(stateEngine, "ArtWorkImageFormat");
        addJsonData(stateEngine, "ArtWorkImageType");
        addJsonData(stateEngine, "ArtworkRecipe");
        addJsonData(stateEngine, "AssetMetaDatas", "AssetMetaDatasTrackLabelsTranslatedTexts");
        addJsonData(stateEngine, "VideoRights", "VideoRightsFlagsFirstDisplayDates");
        addJsonData(stateEngine, "Awards", "AwardsAlternateNameTranslatedTexts", "AwardsAwardNameTranslatedTexts");
        addJsonData(stateEngine, "Bcp47Code");
        addJsonData(stateEngine, "CacheDeploymentIntent");
        addJsonData(stateEngine, "Categories", "CategoriesDisplayNameTranslatedTexts", "CategoriesShortNameTranslatedTexts");
        addJsonData(stateEngine, "CategoryGroups", "CategoryGroupsCategoryGroupNameTranslatedTexts");
        addJsonData(stateEngine, "Cdns");
        addJsonData(stateEngine, "Certifications", "CertificationsNameTranslatedTexts", "CertificationsDescriptionTranslatedTexts");
        addJsonData(stateEngine, "CertificationSystem");
        addJsonData(stateEngine, "Character");
        addJsonData(stateEngine, "CharacterArtwork");
        addJsonData(stateEngine, "Characters", "CharactersBTranslatedTexts", "CharactersCnTranslatedTexts");
        addJsonData(stateEngine, "ConsolidatedCertificationSystems", "ConsolidatedCertificationSystemsRatingDescriptionsTranslatedTexts", "ConsolidatedCertificationSystemsNameTranslatedTexts", "ConsolidatedCertificationSystemsDescriptionTranslatedTexts", "ConsolidatedCertificationSystemsRatingRatingCodesTranslatedTexts");
        addJsonData(stateEngine, "ConsolidatedVideoRatings", "ConsolidatedVideoRatingsRatingsCountryRatingsReasonsTranslatedTexts");
        addJsonData(stateEngine, "CSMReview");
        addJsonData(stateEngine, "DefaultExtensionRecipe");
        addJsonData(stateEngine, "DeployablePackages");
        addJsonData(stateEngine, "DrmSystemIdentifiers");
        addJsonData(stateEngine, "Episodes", "EpisodesEpisodeNameTranslatedTexts");
        addJsonData(stateEngine, "Festivals", "FestivalsCopyrightTranslatedTexts", "FestivalsShortNameTranslatedTexts", "FestivalsFestivalNameTranslatedTexts", "FestivalsDescriptionTranslatedTexts", "FestivalsSingularNameTranslatedTexts");
        addJsonData(stateEngine, "Languages", "LanguagesNameTranslatedTexts");
        addJsonData(stateEngine, "LocalizedCharacter", "LocalizedCharacterTranslatedTexts");
        addJsonData(stateEngine, "LocalizedMetadata", "LocalizedMetadataTranslatedTexts");
        addJsonData(stateEngine, "MovieRatings", "MovieRatingsRatingReasonTranslatedTexts");
        addJsonData(stateEngine, "Movies", "MoviesTvSynopsisTranslatedTexts", "MoviesSiteSynopsisTranslatedTexts", "MoviesShortDisplayNameTranslatedTexts", "MoviesDisplayNameTranslatedTexts", "MoviesOriginalTitleTranslatedTexts", "MoviesTransliteratedTranslatedTexts", "MoviesAkaTranslatedTexts");
        addJsonData(stateEngine, "OriginServers");
        addJsonData(stateEngine, "PersonAliases", "PersonAliasesNameTranslatedTexts");
        addJsonData(stateEngine, "PersonArtwork");
        addJsonData(stateEngine, "Persons", "PersonsNameTranslatedTexts", "PersonsBioTranslatedTexts");
        addJsonData(stateEngine, "ProtectionTypes");
        addJsonData(stateEngine, "Ratings", "RatingsDescriptionTranslatedTexts", "RatingsRatingCodeTranslatedTexts");
        addJsonData(stateEngine, "Rollout", "RolloutLaunchDates", "RolloutPhasesWindows", "RolloutPhasesElementsTrailersSupplementalInfo");
        addJsonData(stateEngine, "ShowMemberTypes", "ShowMemberTypesDisplayNameTranslatedTexts");
        addJsonData(stateEngine, "StorageGroups");
        addJsonData(stateEngine, "Stories_Synopses", "Stories_SynopsesHooksTranslatedTexts", "Stories_SynopsesNarrativeTextTranslatedTexts");
        addJsonData(stateEngine, "StreamProfileGroups");
        addJsonData(stateEngine, "StreamProfiles");
        addJsonData(stateEngine, "TerritoryCountries");
        addJsonData(stateEngine, "TopN");
        addJsonData(stateEngine, "Trailer");
        //addJsonData(stateEngine, "TurboCollections");
        addJsonData(stateEngine, "VideoArtWork");
        addJsonData(stateEngine, "VideoAward");
        addJsonData(stateEngine, "VideoDate");
        addJsonData(stateEngine, "VideoDisplaySet");
        addJsonData(stateEngine, "VideoGeneral");
        addJsonData(stateEngine, "VideoPerson");
        addJsonData(stateEngine, "VideoRating");
        addJsonData(stateEngine, "VideoType");
        addJsonData(stateEngine, "VMSAward");

        addJsonData(stateEngine, "VideoAward");
        addJsonData(stateEngine, "VideoDate");
        addJsonData(stateEngine, "VideoDisplaySet");
        addJsonData(stateEngine, "VideoGeneral");
        addJsonData(stateEngine, "VideoPerson");
        addJsonData(stateEngine, "VideoRating");
        addJsonData(stateEngine, "VideoType");

        HollowBlobWriter writer = new HollowBlobWriter(stateEngine);
        BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream("/tmp/VMSInputData.hollow"));
        writer.writeSnapshot(os);
        os.close();

    }

    private PrintWriter writer;

    private void discoverJsonSchemas(String type, String... mapTypes) throws Exception {
        HollowJsonAdapterSchemaDiscoverer schemaDiscoverer = new HollowJsonAdapterSchemaDiscoverer(type);
        schemaDiscoverer.addMapTypes(new HashSet<String>(Arrays.asList(mapTypes)));

        Collection<HollowDiscoveredSchema> discoveredSchemas = schemaDiscoverer.discoverSchemas(getFile(type), Integer.MAX_VALUE);

        writer.println("////////////////////////// " + type + " //////////////////////////");
        writer.println();

        for(HollowDiscoveredSchema discoveredSchema : discoveredSchemas) {
            writer.println(discoveredSchema.toHollowSchema());
            writer.println();
        }

        writer.flush();

    }


    private void addJsonData(HollowWriteStateEngine stateEngine, String type, String... mapTypes) throws Exception {
        System.out.println("ADDING: " + type);

        //discoverJsonSchemas(type,  mapTypes);
        //if(true) return;


        HollowJsonAdapterStateEnginePopulator populator = new HollowJsonAdapterStateEnginePopulator(stateEngine, type);
        populator.populate(getFile(type));
    }

    private File getFile(String type) {
        File jsonFile = new File("/space/coldstarts/" + type + ".json_V1");
        if(!jsonFile.exists())
            jsonFile = new File("/space/coldstarts/" + type + ".json_v1");
        return jsonFile;
    }

    private Collection<HollowSchema> loadSchemas() throws IOException {
        Reader reader = new InputStreamReader(this.getClass().getResourceAsStream("/schemas.txt"));
        String schemaDefinition = IOUtils.toString(reader);
        return HollowSchemaParser.parseCollectionOfSchemas(schemaDefinition);
    }

}
