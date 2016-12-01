import java.util.zip.GZIPOutputStream;

import com.netflix.hollow.write.HollowBlobWriter;
import com.netflix.hollow.write.objectmapper.HollowObjectMapper;
import com.netflix.hollow.write.objectmapper.HollowHashKey;
import com.netflix.hollow.write.HollowWriteStateEngine;
import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Test;


public class TestMovieGeneration {

    @Test
    public void bootstrap() throws IOException {
        Random rand = new Random();
        
        List<Actor> allActors = new ArrayList<Actor>();
        
        int actorId = 1000000;
        
        for(int i=1;i<1000;i++) {
            actorId++;
            
            allActors.add(new Actor(actorId, generateRandomString()));
        }
        
        int movieId = 1000000;
        
        List<Movie> allMovies = new ArrayList<Movie>();

        HollowWriteStateEngine writeEngine = new HollowWriteStateEngine();
        HollowObjectMapper hollowMapper = new HollowObjectMapper(writeEngine);

        for(int i=0;i<10000;i++) {
            movieId++;
            
            int numActors = rand.nextInt(25) + 1;
            Set<Actor> actors = new HashSet<Actor>();
            
            for(int j=0;j<numActors;j++) {
                actors.add(allActors.get(rand.nextInt(allActors.size())));
            }
            
            Movie movie = new Movie(movieId, generateRandomString(), actors);
            allMovies.add(movie);
            hollowMapper.addObject(movie);
        }
        

        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        try (OutputStream os = new BufferedOutputStream(new FileOutputStream("/tmp/movies.json"))) {
            mapper.writeValue(os, allMovies);
        }
        
        try (OutputStream os = new BufferedOutputStream(new FileOutputStream("/tmp/movies.hollow"))) {
            HollowBlobWriter writer = new HollowBlobWriter(writeEngine);
            writer.writeSnapshot(os);
        }
    }

    private String generateRandomString() {
        Random rand = new Random();
        
        StringBuilder str = new StringBuilder();
        int nameChars = rand.nextInt(20) + 5;
        
        for(int j=0;j<nameChars;j++) {
            str.append((char)(rand.nextInt(26) + 97));
        }
        return str.toString();
    }
    
    
    
    public static class Movie {
        public int id;
        public String title;
        @HollowHashKey(fields="actorId")
        public Set<Actor> actors;
        
        public Movie(int id, String title, Set<Actor> actors) {
            this.id = id;
            this.title = title;
            this.actors = actors;
        }
    }
    
    public static final class Actor {
        public final int actorId;
        public final String actorName;
        
        public Actor(int actorId, String actorName) {
            this.actorId = actorId;
            this.actorName = actorName;
        }
    }
    
    
}
