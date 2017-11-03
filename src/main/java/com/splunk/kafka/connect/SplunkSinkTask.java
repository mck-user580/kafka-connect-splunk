package com.splunk.kafka.connect;

import com.splunk.hecclient.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kchen on 9/21/17.
 */
public final class SplunkSinkTask extends SinkTask implements PollerCallback {
    private static final Logger log = LoggerFactory.getLogger(SplunkSinkTask.class);
    private static final int backPressureResetWindow = 10 * 60 * 1000; // 10 mins

    private HecInf hec;
    private KafkaRecordTracker tracker;
    private long backPressure = 0;
    private long lastResetTime = System.currentTimeMillis();
    private SplunkSinkConnectorConfig connectorConfig;

    @Override
    public void start(Map<String, String> taskConfig) {
        connectorConfig = new SplunkSinkConnectorConfig(taskConfig);
        hec = createHec();
        tracker = new KafkaRecordTracker();

        log.info("kafka-connect-splunk task starts with config={}", connectorConfig);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.debug("received {} records with backPressure={}", records.size(), backPressure);

        handleBackPressure();
        handleFailedBatches();

        if (records.isEmpty()) {
            return;
        }

        if (connectorConfig.raw) {
            /* /raw endpoint */
            handleRaw(records);
        } else {
            /* /event endpoint */
            handleEvent(records);
        }
    }

    private void handleBackPressure() {
        long curTime = System.currentTimeMillis();

        if (backPressure > 0) {
            backPressure = 0;
            lastResetTime = curTime;
            throw new RetriableException(new HecException("detected backPressure, pause the pull for a while"));
        }

        if (curTime - lastResetTime > backPressureResetWindow) {
            // 10 mins
            backPressure = 0;
            lastResetTime = curTime;
        }
    }

    private void handleFailedBatches() {
        Collection<EventBatch> failed = tracker.getAndRemoveFailedRecords();
        log.info("handle {} failed batches", failed.size());
        // if there are failed ones, first deal with them
        for (final EventBatch batch: failed) {
            send(batch);
        }

        if (!failed.isEmpty()) {
            throw new RetriableException(new HecException("need handle failed batches first, pause the pull for a while"));
        }
    }

    private void handleRaw(final Collection<SinkRecord> records) {
        if (connectorConfig.hasMetaDataConfigured()) {
            // when setup metadata - index, source, sourcetype, we need partition records for /raw
            Map<TopicPartition, Collection<SinkRecord>> partitionedRecords = partitionRecords(records);
            for (Map.Entry<TopicPartition, Collection<SinkRecord>> entry: partitionedRecords.entrySet()) {
                EventBatch batch = createRawEventBatch(entry.getKey());
                sendEvents(entry.getValue(), batch);
            }
        } else {
            EventBatch batch = createRawEventBatch(null);
            sendEvents(records, batch);
        }
    }

    private void handleEvent(final Collection<SinkRecord> records) {
        EventBatch batch = new JsonEventBatch();
        sendEvents(records, batch);
    }

    private void sendEvents(final Collection<SinkRecord> records, EventBatch batch) {
        for (final SinkRecord record: records) {
            Event event;
            try {
                event = createHecEventFrom(record);
            } catch (HecException ex) {
                log.info("ignore null or empty event for topicPartition={}-{}", record.topic(), record.kafkaPartition());
                continue;
            }

            batch.add(event);
            if (batch.size() >= connectorConfig.maxBatchSize) {
                send(batch);
                // start a new batch after send
                batch = batch.createFromThis();
            }
        }

        // Last batch
        if (!batch.isEmpty()) {
            send(batch);
        }
    }

    private void send(final EventBatch batch) {
        batch.resetSendTimestamp();
        tracker.addEventBatch(batch);
        boolean success = hec.send(batch);
        if (success) {
            backPressure -= 1;
        } else {
            backPressure += 1;
        }
    }

    // setup metadata on RawEventBatch
    private EventBatch createRawEventBatch(final TopicPartition tp) {
        if (tp == null) {
            return RawEventBatch.factory().build();
        }

        Map<String, String> metas = connectorConfig.topicMetas.get(tp.topic());
        if (metas == null) {
            return RawEventBatch.factory().build();
        }

        return RawEventBatch.factory()
                .setIndex(metas.get(SplunkSinkConnectorConfig.INDEX))
                .setSourcetype(metas.get(SplunkSinkConnectorConfig.SOURCETYPE))
                .setSource(metas.get(SplunkSinkConnectorConfig.SOURCE))
                .build();
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> meta) {
        // tell Kafka Connect framework what are offsets we can safely commit to Kafka now
        Map<TopicPartition, OffsetAndMetadata> offsets = tracker.computeOffsets();
        log.debug("commits offsets offered={}, pushed={}", offsets, meta);
        return meta;
    }

    @Override
    public void stop() {
        if (hec != null) {
            hec.close();
        }
        log.info("kafka-connect-splunk task ends with config={}", connectorConfig);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    public void onEventCommitted(final List<EventBatch> batches) {
        for (final EventBatch batch: batches) {
            assert batch.isCommitted();
        }
    }

    public void onEventFailure(final List<EventBatch> batches, Exception ex) {
        for (EventBatch batch: batches) {
            tracker.addFailedEventBatch(batch);
        }
    }

    private Event createHecEventFrom(SinkRecord record) {
        if (connectorConfig.raw) {
            return new RawEvent(record.value(), record);
        }

        // meta data for /event endpoint is per event basis
        JsonEvent event = new JsonEvent(record.value(), record);
        Map<String, String> metas = connectorConfig.topicMetas.get(record.topic());
        if (metas != null) {
            event.setIndex(metas.get(SplunkSinkConnectorConfig.INDEX));
            event.setSourcetype(metas.get(SplunkSinkConnectorConfig.SOURCETYPE));
            event.setSource(metas.get(SplunkSinkConnectorConfig.SOURCE));
            event.addFields(connectorConfig.enrichements);
        }

        return event;
    }

    // partition records according to topic-partition key
    private Map<TopicPartition, Collection<SinkRecord>> partitionRecords(Collection<SinkRecord> records) {
        Map<TopicPartition, Collection<SinkRecord>> partitionedRecords = new HashMap<>();

        for (SinkRecord record: records) {
            TopicPartition key = new TopicPartition(record.topic(), record.kafkaPartition());
            Collection<SinkRecord> partitioned = partitionedRecords.get(key);
            if (partitioned == null) {
                partitioned = new ArrayList<>();
                partitionedRecords.put(key, partitioned);
            }
            partitioned.add(record);
        }
        return partitionedRecords;
    }

    private HecInf createHec() {
        if (connectorConfig.numberOfThreads > 1) {
            return new ConcurrentHec(connectorConfig.numberOfThreads, connectorConfig.ack,
                    connectorConfig.getHecConfig(), this);
        } else {
            if (connectorConfig.ack) {
                return Hec.newHecWithAck(connectorConfig.getHecConfig(), this);
            } else {
                return Hec.newHecWithoutAck(connectorConfig.getHecConfig(), this);
            }
        }
    }
}