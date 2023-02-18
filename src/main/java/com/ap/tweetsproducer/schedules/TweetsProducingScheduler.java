package com.ap.tweetsproducer.schedules;

import com.ap.model.Tweet;
import com.ap.tweetsproducer.config.KafkaConfig;
import com.ap.tweetsproducer.config.TwitterConfig;
import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.model.Get2TweetsSearchRecentResponse;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class TweetsProducingScheduler {

    private static final Logger
            log = LoggerFactory.getLogger(TweetsProducingScheduler.class);
    private final TwitterConfig twitterConfig;
    private final KafkaConfig kafkaConfig;

    @Autowired
    public TweetsProducingScheduler(TwitterConfig twitterConfig,
                                    KafkaConfig kafkaConfig) {
        this.twitterConfig = twitterConfig;
        this.kafkaConfig = kafkaConfig;
    }


    @Scheduled(fixedRate = 10000)
    public void reportCurrentTime() {

        Set<String> tweetFields = new HashSet<>();
        tweetFields.add("author_id");
        tweetFields.add("id");
        tweetFields.add("created_at");
        tweetFields.add("text");

        final KafkaProducer<String, Tweet> producer =
                new KafkaProducer<>(kafkaConfig.getKafkaConnectionConfig());

        try {
            Get2TweetsSearchRecentResponse result =
                    twitterConfig.getTwitterApi().tweets().tweetsRecentSearch("bitcoin")
                                                     .tweetFields(tweetFields)
                                                     .execute();
            if(result.getErrors() != null && result.getErrors().size() > 0) {

                log.error("Errors returned from twitter api.");

            } else {

                List<Tweet> tweetsToPublish = result.getData().stream().map(t -> new Tweet(t.getCreatedAt().toEpochSecond()
                        , t.getId(),
                                                                                           t.getAuthorId(),
                                                                                           t.getText())).collect(
                        Collectors.toList());

                log.info("findTweetById - Tweet Text: {}", tweetsToPublish);


                tweetsToPublish.stream().forEach(t -> {
                    ProducerRecord<String, Tweet> producerRecord =
                            new ProducerRecord<>("bitcoin_tweets", t);

                    producer.send(producerRecord);
                });


            }

        } catch (ApiException e) {
            log.error("Exception occurred while fetching tweets" , e);
        }
    }
}
