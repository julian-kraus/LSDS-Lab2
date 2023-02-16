package edu.upf.model;

import java.util.Optional;
import java.io.FileReader;
import java.io.StringReader;
import java.io.IOException; 

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.JsonParseException;


public class SimplifiedTweet {

    // members
    private final long tweetId;  // the id of the tweet (’id’)
    private final String text;   // the content of the tweet (’text’)
    private final long userId;   // the user id (’user->id’)
    private final String userName; // the user name (’user’->’name’)
    private final String language; // the language of a tweet (’lang’)
    private final long timestampMs; // seconds from epoch (’timestamp_ms’)

    // Constructor
    public SimplifiedTweet(long tweetId, String text, long userId, String userName, String language, long timestampMs) {
            this.tweetId = tweetId;
            this.text = text;
            this.userId = userId;
            this.userName = userName;
            this.language = language;
            this.timestampMs = timestampMs;
        }


    /**
    * Returns a {@link SimplifiedTweet} from a JSON String.
    * If parsing fails, for any reason, return an {@link Optional#empty()}
    *
    * @param jsonStr
    * @return an {@link Optional} of a {@link SimplifiedTweet}
    */

    public static Optional<SimplifiedTweet> fromJson(String jsonStr) throws IOException{
	
        SimplifiedTweet tweet = null;
        
        try{
            
          JsonElement je = JsonParser.parseString(jsonStr);
          Optional <JsonElement> opt_je = Optional.ofNullable(je);
          JsonObject  jo = opt_je.get().getAsJsonObject();
          
          if(jo.has("id") &&
           jo.has("user") &&
           jo.has("text") &&
           jo.has("lang") && 
           jo.has("timestamp_ms")){
            JsonObject userObj = jo.getAsJsonObject("user");
            Long tweetId = jo.get("id").getAsLong();
            String text= jo.get("text").getAsString();
            Long timeStamp = jo.get("timestamp_ms").getAsLong();
            String lang = jo.get("lang").getAsString();
            if(
              userObj.has("id") && 
              userObj.has("name")
              ){
                Long userId = userObj.get("id").getAsLong();
                String userName = userObj.get("name").getAsString();    
              tweet = new SimplifiedTweet(tweetId, text, userId, userName, lang, timeStamp);
              return Optional.ofNullable(tweet);
            }
          }
          return Optional.empty();
          } catch(Exception e){
          return Optional.empty();
        }
      }

    public String getLanguage() {
        return this.language;
    }

    @Override
    public String toString() {
        // Overriding how SimplifiedTweets are printed in console or the output file
        // The following line produces valid JSON as output
        return new Gson().toJson(this);
    }

}
