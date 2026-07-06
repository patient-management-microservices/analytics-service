package com.pm.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Slf4j
@Service
public class KafkaConsumer {

    @KafkaListener(topics = "patient", groupId = "analytics-service")
    public void consumeEvent(byte[] event) {
        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(event);
            // perform any business logic related to analytics here
            log.info("Received patient event: [patientId = {}, name = {}, email = {}, eventType = {}]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail(),
                    patientEvent.getEventType());
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse PatientEvent from Kafka message: {}", e.getMessage());
        }

    }
}