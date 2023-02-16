package spark;

import java.util.Optional;
import java.io.IOException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.JsonParseException;

public class ExtendedSimplifiedTweet {

    // members
    private final long tweetId;  // the id of the tweet (’id’)
    private final String text;   // the content of the tweet (’text’)
    private final long userId;   // the user id (’user->id’)
    private final String userName; // the user name (’user’->’name’)
    private final long followersCount; // the number of followers (’user’->’followers_count’)
    private final String language; // the language of a tweet (’lang’)
    private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
    private final Long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final Long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id)
    private final long timestampMs; // seconds from epoch (’timestamp_ms’)

    // Constructor
    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName, long followersCount, String language, boolean isRetweeted, Long retweetedUserId, Long retweetedTweetId, long timestampMs) {
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.followersCount = followersCount;
        this.language = language;
        this.isRetweeted = isRetweeted;
        this.retweetedUserId = retweetedUserId;
        this.retweetedTweetId = retweetedTweetId;
        this.timestampMs = timestampMs;
        }

        // getter for text
    public String getText(){
        return this.text;
    }


    /**
    * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
    * If parsing fails, for any reason, return an {@link Optional#empty()}
    *
    * @param jsonStr
    * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
    */

    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) throws IOException{
	
        ExtendedSimplifiedTweet tweet = null;
        
        try{
            
          JsonElement je = JsonParser.parseString(jsonStr);
          Optional <JsonElement> opt_je = Optional.ofNullable(je);
          JsonObject  jo = opt_je.get().getAsJsonObject();
          Long retweetedUserId = 0L;
          Long retweetedTweetId = 0L;
          if(jo.has("id") &&
           jo.has("user") &&
           jo.has("text") &&
           jo.has("lang") && 
           jo.has("timestamp_ms") &&
           jo.has("retweeted")
          ){
              boolean isRetweeted = jo.get("retweeted").getAsBoolean();
              if (isRetweeted) {
                  JsonObject retweetedTweet = jo.getAsJsonObject("retweeted_status");
                  if (retweetedTweet.has("id") &&
                        retweetedTweet.has("user")) {
                      retweetedTweetId = retweetedTweet.get("id").getAsLong();
                      JsonObject ruo = retweetedTweet.getAsJsonObject("user");
                      if (ruo.has("id")) {
                          retweetedUserId = ruo.get("id").getAsLong();
                      }
                  }
              }
              // if retweeted, get retweeted user id/tweet Id
            JsonObject userObj = jo.getAsJsonObject("user");
            Long tweetId = jo.get("id").getAsLong();
            String text= jo.get("text").getAsString();
            Long timeStamp = jo.get("timestamp_ms").getAsLong();
            String lang = jo.get("lang").getAsString();
            if(
              userObj.has("id") && 
              userObj.has("name") &&
                      userObj.has("followers_count")
              ){
                Long followersCount = userObj.get("followers_count").getAsLong();
                Long userId = userObj.get("id").getAsLong();
                String userName = userObj.get("name").getAsString();    
              tweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount, lang, isRetweeted, retweetedUserId, retweetedTweetId, timeStamp);
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

    public boolean isRetweeted() {
        return isRetweeted;
    }

    public Long getRetweetedUserId() {
        return retweetedUserId;
    }

    public Long getRetweetedTweetId() {
        return retweetedTweetId;
    }
}
