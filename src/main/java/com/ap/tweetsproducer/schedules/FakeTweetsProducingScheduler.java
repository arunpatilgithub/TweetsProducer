package com.ap.tweetsproducer.schedules;


import com.ap.model.Tweet;
import com.ap.tweetsproducer.config.KafkaConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.datafaker.Faker;
import net.datafaker.providers.base.Twitter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Locale;

@Component
public class FakeTweetsProducingScheduler {

    private final KafkaConfig kafkaConfig;

    private static final Logger
            log = LoggerFactory.getLogger(FakeTweetsProducingScheduler.class);

    public FakeTweetsProducingScheduler(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Scheduled(fixedRate = 10000)
    public void publishFakeTweets() throws JsonProcessingException {

        Faker faker = new Faker(Locale.ENGLISH);
        Twitter twitter = faker.twitter();
        Tweet tweet = new Tweet(twitter.createdTime(true, new Date(),
                                                    new Date(2524608000000l)).getTime(),
                                twitter.twitterId(16),
                                twitter.userId(),
                                twitter.text(new String[]{"bitcoin"},5,25));

        final KafkaProducer<String, String> producer =
                new KafkaProducer<>(kafkaConfig.getKafkaConnectionConfig());

        log.info("Fake tweet to publish : {}" , tweet);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("bitcoin_tweets_fake",
                                     tweet.toString());

        producer.send(producerRecord);
    }
}
