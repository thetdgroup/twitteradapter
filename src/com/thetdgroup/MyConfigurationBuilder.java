package com.thetdgroup;

import java.io.InputStream;
import java.util.Properties;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class MyConfigurationBuilder
{
 private final static String TWITTER_CONSUMERKEY = "oauth.consumerKey";
 private final static String TWITTER_CONSUMERSECRET = "oauth.consumerSecret";
 private final static String TWITTER_ACCESSTOKEN = "oauth.accessToken";
 private final static String TWITTER_ACCESSTOKENSECRET = "oauth.accessTokenSecret";

 private ConfigurationBuilder cb = null; 
 private Twitter twitter = null;

 //
 public MyConfigurationBuilder() throws Exception 
 {
  Properties prop = new Properties();
  InputStream in = TwitterAdapter.class.getClassLoader().getResourceAsStream("resource/twitter4j.properties");
  
  if(in == null)
  {
   throw new Exception("File twitter4j.properties not found");
  }
  
  prop.load(in);
  in.close();
  
  //
  cb = new ConfigurationBuilder();
  cb.setDebugEnabled(true)
    .setOAuthConsumerKey(prop.getProperty(TWITTER_CONSUMERKEY))
    .setOAuthConsumerSecret(prop.getProperty(TWITTER_CONSUMERSECRET))
    .setOAuthAccessToken(prop.getProperty(TWITTER_ACCESSTOKEN))
    .setOAuthAccessTokenSecret(prop.getProperty(TWITTER_ACCESSTOKENSECRET))
    .setGZIPEnabled(true)
    .setJSONStoreEnabled(true);
  
   TwitterFactory tf = new TwitterFactory(cb.build());
   twitter = tf.getInstance();
 }
}
