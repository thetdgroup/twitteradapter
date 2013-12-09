package com.thetdgroup;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MapReduceOutput;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;

import org.json.JSONException;
import org.json.JSONObject;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import java.io.InputStream;
import java.net.URI;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

//
public class TwitterAdapter
{
 private static MongoDBAdapter mongoDBAdapter = null;
 private static String mongoDBCollectionName = "twitter";
 
 private Properties properties = new Properties();
 
 private final static String TWITTER_CONSUMERKEY = "oauth.consumerKey";
 private final static String TWITTER_CONSUMERSECRET = "oauth.consumerSecret";
 private final static String TWITTER_ACCESSTOKEN = "oauth.accessToken";
 private final static String TWITTER_ACCESSTOKENSECRET = "oauth.accessTokenSecret";

 //
 public static void main(String[] args)
 {
  //
  JSONObject connectParams = new JSONObject();
  
  try
  {
  /* connectParams.put("database_name", "TDG_Datawarehouse");
   
   connectParams.put("db_host", "localhost");
   connectParams.put("db_port", "27017");
   
   mongoDBAdapter = new MongoDBAdapter();
   mongoDBAdapter.connect(connectParams);*/
   
   //
   /*JSONObject collectionParams = new JSONObject();
   collectionParams.put("collection_name", mongoDBCollectionName);
   
   db.createCollection(collectionParams);*/
   
   //
   TwitterAdapter twitterInjector = new TwitterAdapter();
   twitterInjector.setUp();
   twitterInjector.injectTweets();
  }
  //catch (JSONException e)
  //{
  // e.printStackTrace();
 // }
  catch (Exception e)
  {
   e.printStackTrace();
  }
  finally
  {
  // db.disconnect();
  }
 }

 //
 private void setUp()
 {
  try
  {
   properties = new Properties();
   InputStream inputStream = TwitterAdapter.class.getClassLoader().getResourceAsStream("resource/twitter4j.properties");
   
   if(inputStream == null)
   {
    throw new Exception("File twitter4j.properties not found");
   }
   
   properties.load(inputStream);
   inputStream.close();

   /*if(prop.containsKey(COUCHBASE_URIS))
   {
    String[] uriStrings = prop.getProperty(COUCHBASE_URIS).split(",");
    
    for(int i = 0; i < uriStrings.length; i++)
    {
     couchbaseServerUris.add(new URI(uriStrings[i]));
    }

   }
   else
   {
    couchbaseServerUris.add(new URI("http://127.0.0.1:8091/pools"));
   }

   if(prop.containsKey(COUCHBASE_BUCKET))
   {
    couchbaseBucket = prop.getProperty(COUCHBASE_BUCKET);
   }

   if(prop.containsKey(COUCHBASE_PASSWORD))
   {
    couchbasePassword = prop.getProperty(COUCHBASE_PASSWORD);
   }*/
  }
  catch(Exception e)
  {
   System.out.println(e.getMessage());
   System.exit(0);
  }
 }

 //
 private void injectTweets()
 {
  //
  //
  //
  ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
  configurationBuilder.setDebugEnabled(true);
  configurationBuilder.setGZIPEnabled(true);
  configurationBuilder.setJSONStoreEnabled(true);
  configurationBuilder.setOAuthConsumerKey(properties.getProperty(TWITTER_CONSUMERKEY));
  configurationBuilder.setOAuthConsumerSecret(properties.getProperty(TWITTER_CONSUMERSECRET));
  configurationBuilder.setOAuthAccessToken(properties.getProperty(TWITTER_ACCESSTOKEN));
  configurationBuilder.setOAuthAccessTokenSecret(properties.getProperty(TWITTER_ACCESSTOKENSECRET));

  Configuration configuration = configurationBuilder.build();
  
  //
  // 
  //
  TwitterStream twitterStream = new TwitterStreamFactory(configuration).getInstance();
  Twitter twitter = new TwitterFactory(configuration).getInstance(); 
  
  try
  {
   //final CouchbaseClient cbClient = new CouchbaseClient(couchbaseServerUris, couchbaseBucket, couchbasePassword);
   //System.out.println("Send data to : " + couchbaseServerUris + "/" + couchbaseBucket);
   
   StatusListener statusListener = new StatusListener() {
    
    //
    public void onStatus(Status status)
    {
    // System.out.println(status.getUser().getId() + " : " + status.getUser().getName() + " : " + status.getText());
     String twitterMessage = DataObjectFactory.getRawJSON(status);
     
     if(twitterMessage != null)
     {
      try
      {
       JSONObject jsonTwitterObject = new JSONObject(twitterMessage);
       
       if(jsonTwitterObject.getString("lang").equals("en") /*&&
        jsonTwitterObject.getJSONObject("place").getString("country_code").equals("US")*/)
       {
        System.out.println(jsonTwitterObject.getJSONObject("user").getString("name") + " -- " + jsonTwitterObject);
       // mongoDBAdapter.addDocument(mongoDBCollectionName, jsonTwitterObject);
       }
      }
      catch (Exception e)
      {
       e.printStackTrace();
      }
     }
    }

    //
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice)
    {
    }

    //
    public void onTrackLimitationNotice(int numberOfLimitedStatuses)
    {
    }

    //
    public void onScrubGeo(long userId, long upToStatusId)
    {
    }

    //
    public void onStallWarning(StallWarning arg0)
    {
    }

    //
    public void onException(Exception ex)
    {
     ex.printStackTrace();
    }
   };

   //
   //
   //
   UserStreamListener userStreamListener = new UserStreamListener() {

    @Override
    public void onDeletionNotice(StatusDeletionNotice arg0)
    {
    }

    @Override
    public void onScrubGeo(long arg0, long arg1)
    {
    }

    @Override
    public void onStallWarning(StallWarning arg0)
    {
    }

    @Override
    public void onStatus(Status arg0)
    {
    }

    @Override
    public void onTrackLimitationNotice(int arg0)
    {
    }

    @Override
    public void onException(Exception arg0)
    {
    }

    @Override
    public void onBlock(User arg0, User arg1)
    {
    }

    @Override
    public void onDeletionNotice(long arg0, long arg1)
    {
    }

    @Override
    public void onDirectMessage(DirectMessage arg0)
    {
    }

    @Override
    public void onFavorite(User arg0, User arg1, Status arg2)
    {
    }

    @Override
    public void onFollow(User arg0, User arg1)
    {
    }

    @Override
    public void onFriendList(long[] arg0)
    {
    }

    @Override
    public void onUnblock(User arg0, User arg1)
    {
    }

    @Override
    public void onUnfavorite(User arg0, User arg1, Status arg2)
    {
    }

    @Override
    public void onUserListCreation(User arg0, UserList arg1)
    {
    }

    @Override
    public void onUserListDeletion(User arg0, UserList arg1)
    {
    }

    @Override
    public void onUserListMemberAddition(User arg0, User arg1, UserList arg2)
    {
    }

    @Override
    public void onUserListMemberDeletion(User arg0, User arg1, UserList arg2)
    {
    }

    @Override
    public void onUserListSubscription(User arg0, User arg1, UserList arg2)
    {
    }

    @Override
    public void onUserListUnsubscription(User arg0, User arg1, UserList arg2)
    {
    }

    @Override
    public void onUserListUpdate(User arg0, UserList arg1)
    {
    }

    @Override
    public void onUserProfileUpdate(User arg0)
    {
    }
   };
   
   //
   // Add the listeners
   //
   twitterStream.addListener(statusListener);
   twitterStream.addListener(userStreamListener);
   
   //
   // Add location filter for what I hope is the whole planet. Just trying to limit
   // results to only things that are geotagged
   //
   double[][] locations = {{-180.0d,-90.0d},{180.0d,90.0d}};
 
   FilterQuery locationFilter = new FilterQuery();
   locationFilter.locations(locations);
   twitterStream.filter(locationFilter);
   
   //
   // Look for specific items
   //
   //FilterQuery aFilter = new FilterQuery();
   //String[] itemsToTrack = {"James DiMaggio"};
   //aFilter.track(itemsToTrack);
   //twitterStream.filter(aFilter);
   
   //
   // Track users (?)
   //
  // String[] usersToTrack = {"@edamphousse"};
   //twitterStream.user();
   
   //
   // Do some sampling 
   //
   // twitterStream.sample();
  }
  catch (Exception e)
  {
   e.printStackTrace();
  }
 }
}
