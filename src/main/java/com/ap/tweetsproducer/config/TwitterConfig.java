package com.ap.tweetsproducer.config;

import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.TwitterCredentialsOAuth2;
import com.twitter.clientlib.api.TwitterApi;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TwitterConfig {

    public TwitterApi getTwitterApi() {

        return new TwitterApi(new TwitterCredentialsBearer(System.getenv(
                "BEARER")));
    }


}
