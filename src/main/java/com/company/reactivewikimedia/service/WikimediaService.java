package com.company.reactivewikimedia.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

@Service
public class WikimediaService {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;


    public WikimediaService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.wikimediaFlux = Flux.create(emitter -> this.fluxSink = emitter);
    }

    private Flux<String> wikimediaFlux;
    private FluxSink<String> fluxSink;

    public void sendKafkaWikimediaMessage() {
        WebClient client = WebClient.create("https://stream.wikimedia.org/v2/stream/test");

        Flux<String> wikimediaFlux = client.get()
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(String.class);

        wikimediaFlux.subscribe(event -> kafkaTemplate.send("wikimedia-topic", event));

    }

    @KafkaListener(
            topics = "wikimedia-topic",
            groupId = "wikimedia-group"
    )
    public void getWikimediaEvent(
            String message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("Kafka Consume edildi ... mesaj {}", message);

        if(fluxSink!=null){
            fluxSink.next(message);
        }

    }

    public Flux<String> getWikimediaFlux() {
        return wikimediaFlux;
    }

}
