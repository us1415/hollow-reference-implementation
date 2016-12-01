import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hollow.jsonadapter.util.JsonUtil;

public class JsonPrintPrintTest {

    @Test
    public void printJson() throws Exception {
        String file = "/space/coldstarts/jsons/beehive.zip/StreamProfiles.json_v1";

        InputStream in = new FileInputStream(new File(file));
        JsonParser parser = new JsonFactory().createParser(in);
        PrintStream out = new PrintStream(new FileOutputStream(file + ".pretty.json"));
        JsonUtil.print(parser, out);

        in.close();
        out.close();
    }

    @Test
    public void debugJsonParser() throws Exception {
        //InputStream in = new ByteArrayInputStream("{ \"name\" : \"test\"}".getBytes(Charset.forName("UTF-8")));
        String testFile = "/tmp/R2.json";
        String realFile = "/space/coldstarts/jsons/beehive.zip/Rollout.json_V1";
        {
            InputStream in = new FileInputStream(new File(testFile));
            JsonParser parser = new JsonFactory().createParser(in);
            JsonUtil.print(parser);
        }

        {
            InputStream in = new FileInputStream(new File(realFile));
            JsonParser parser = new JsonFactory().createParser(in);

            int count = 0;
            JsonToken token = parser.nextToken();
            while (token != null) {
                System.out.println("------ token=" + token);
                if (token == JsonToken.START_OBJECT) {
                    final ObjectMapper mapper = new ObjectMapper();
                    final JsonNode jNode = mapper.readTree(parser);

                    JsonParser jp = jNode.traverse();
                    System.out.println(jNode);

                    JsonUtil.print(jp);
                    if (++count >= 2) break;
                }
                token = parser.nextToken();
            }
        }
    }

}
