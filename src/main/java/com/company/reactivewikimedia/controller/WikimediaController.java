package com.company.reactivewikimedia.controller;

import com.company.reactivewikimedia.service.WikimediaService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/wikimedia")
public class WikimediaController {

    private final WikimediaService wikimediaService;

    public WikimediaController(WikimediaService wikimediaService) {
        this.wikimediaService = wikimediaService;
    }

    @GetMapping
    public ResponseEntity<String> getWikimediaEvent() {
        wikimediaService.sendKafkaWikimediaMessage();
        return ResponseEntity.ok("Islem basarili");
    }

    @GetMapping(value = "/flux", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<Flux<String>> getWikimediaEventKafka() {
        final Flux<String> wikimediaFlux = wikimediaService.getWikimediaFlux();
        return ResponseEntity.ok(wikimediaFlux);
    }
}
