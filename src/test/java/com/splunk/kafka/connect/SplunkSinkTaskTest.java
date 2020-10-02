/*
 * Copyright 2017 Splunk, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splunk.kafka.connect;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.Assert;
import org.junit.Test;

import com.splunk.hecclient.Event;
import com.splunk.hecclient.EventBatch;
import com.splunk.hecclient.JsonEventBatch;
import com.splunk.hecclient.RawEventBatch;

public class SplunkSinkTaskTest {
    @Test
    public void startStopDefault() {
        SplunkSinkTask task = new SplunkSinkTask();
        UnitUtil uu = new UnitUtil(0);
        // shall not throw
        task.stop();

        task.start(uu.createTaskConfig());
        task.stop();
    }

    @Test
    public void startStopWithoutAck() {
        SplunkSinkTask task = new SplunkSinkTask();
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(false));

        task.start(config);
        task.stop();
    }

    @Test
    public void startStopConcurrent() {
        SplunkSinkTask task = new SplunkSinkTask();
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.HEC_THREDS_CONF, "2");

        task.start(config);
        task.stop();
    }

    @Test
    public void putWithEventAndAck() {
      Map<String, String> extraConf = new HashMap<>();
      extraConf.put(SplunkSinkConnectorConfig.INDEXES_CONF, "i1");
      extraConf.put(SplunkSinkConnectorConfig.SOURCETYPES_CONF, "s1");
      extraConf.put(SplunkSinkConnectorConfig.SOURCES_CONF, "e1");
      putWithSuccess(true, true, extraConf);
  
      Map<String, String> conf = new HashMap<>();
      conf.put(SplunkSinkConnectorConfig.INDEXES_CONF, "");
      conf.put(SplunkSinkConnectorConfig.SOURCETYPES_CONF, "");
      conf.put(SplunkSinkConnectorConfig.SOURCES_CONF, "");
      putWithSuccess(true, false, conf);
  
    }

    @Test
    public void putWithoutMaxBatchAligned() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        // success
        hec.setSendReturnResult(HecMock.success);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(120));
        Assert.assertEquals(2, hec.getBatches().size());
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(uu.configProfile.getTopics(), 1), new OffsetAndMetadata(120));
        Assert.assertEquals(offsets, task.preCommit(new HashMap<>()));
        Assert.assertTrue(task.getTracker().getAndRemoveFailedRecords().isEmpty());
        task.stop();
    }

    @Test
    public void putWithFailure() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        // failure
        hec.setSendReturnResult(HecMock.failure);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(1000));
        Assert.assertEquals(10, hec.getBatches().size());
        Assert.assertTrue(task.getTracker().computeOffsets().isEmpty());
        Assert.assertEquals(10, task.getTracker().getAndRemoveFailedRecords().size());

        task.stop();
    }

    @Test(expected = RetriableException.class)
    public void putWithFailureAndBackpressure() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));
        config.put(SplunkSinkConnectorConfig.MAX_OUTSTANDING_EVENTS_CONF, String.valueOf(1000));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        // failure
        hec.setSendReturnResult(HecMock.failure);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(1000));
        task.put(createSinkRecords(1000));

        task.stop();
    }

    @Test(expected = RetriableException.class)
    public void putWithFailureHandleFailedBatches() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));
        config.put(SplunkSinkConnectorConfig.MAX_OUTSTANDING_EVENTS_CONF, String.valueOf(1000));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        // failure
        hec.setSendReturnResult(HecMock.successAndThenFailure);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(1000));
        task.put(createSinkRecords(1000));

        task.stop();
    }


    @Test(expected = RetriableException.class)
    public void putWithMaxEvents() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));
        config.put(SplunkSinkConnectorConfig.MAX_OUTSTANDING_EVENTS_CONF, String.valueOf(1000));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        hec.setSendReturnResult(HecMock.success);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(1000));
        task.put(createSinkRecords(1000));

        task.stop();
    }

    @Test
    public void putWithEmptyRecords() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(100));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        hec.setSendReturnResult(HecMock.success);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(0));
        Assert.assertTrue(hec.getBatches().isEmpty());
        Assert.assertTrue(task.getTracker().computeOffsets().isEmpty());
        Assert.assertTrue(task.getTracker().getAndRemoveFailedRecords().isEmpty());

        task.stop();
    }

    @Test
    public void putWithInvalidEvent() {
        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(6));

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        hec.setSendReturnResult(HecMock.success);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(10, ""));
        Assert.assertEquals(0, hec.getBatches().size());//empty events are ignored see SplunkSinkTask.sendEvents(final Collection<SinkRecord> records, EventBatch batch) {

        task.stop();
    }

    @Test
    public void putWithRawAndAck() {
        Map<String, String> extraConf = new HashMap<>();
        extraConf.put(SplunkSinkConnectorConfig.INDEXES_CONF, "i1");
        extraConf.put(SplunkSinkConnectorConfig.SOURCETYPES_CONF, "s1");
        extraConf.put(SplunkSinkConnectorConfig.SOURCES_CONF, "e1");
        putWithSuccess(true, true, extraConf);

        
    }
    
    @Test
    public void putWithRawAndAckAndNewMeta() {
        Map<String, String> extraConf = new HashMap<>();
        extraConf.put(SinkTask.TOPICS_CONFIG, "mytopic");
        extraConf.put(SinkTask.TOPICS_CONFIG+".mytopic."+ SplunkSinkConnectorConfig.INDEX_CONF, "i1");
        extraConf.put(SinkTask.TOPICS_CONFIG+".mytopic."+ SplunkSinkConnectorConfig.SOURCETYPE_CONF, "s1");
        extraConf.put(SinkTask.TOPICS_CONFIG+".mytopic."+ SplunkSinkConnectorConfig.SOURCE_CONF, "e1");
        putWithSuccess(true, true, extraConf);
        
        
    }
    

    @Test
    public void putWithRawAndAckWithoutMeta() {
      Map<String, String> conf = new HashMap<>();
      conf.put(SplunkSinkConnectorConfig.INDEXES_CONF, "");
      conf.put(SplunkSinkConnectorConfig.SOURCETYPES_CONF, "");
      conf.put(SplunkSinkConnectorConfig.SOURCES_CONF, "");
      putWithSuccess(true, false, conf);
    }
    
    
    
    
    @Test
    public void putWithFieldsFromHeader() throws IOException {
      UnitUtil uu = new UnitUtil(0);
      Map<String, String> config = uu.createTaskConfig();
      config.remove(SplunkSinkConnectorConfig.ENRICHMENT_CONF);
      config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(false));
      config.put(SplunkSinkConnectorConfig.TRACK_DATA_CONF, String.valueOf(false));
      config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(10));

      SplunkSinkTask task = new SplunkSinkTask();
      HecMock hec = new HecMock(task);
      hec.setSendReturnResult(HecMock.success);
      task.setHec(hec);
      task.start(config);

      Collection<SinkRecord> records = createSinkRecords(10);
      for (SinkRecord sr : records) {
        sr.headers().addString(SplunkSinkConnectorConfig.FIELDS_HDR_PREFIX + "testField", "testValue");
        sr.headers().addString(SplunkSinkConnectorConfig.INDEX_HDR,  "kafka");
        sr.headers().addString(SplunkSinkConnectorConfig.SOURCETYPE_HDR,  "testsyslog");
      }

      task.put(records);
      Assert.assertFalse(hec.getBatches().isEmpty());

      for (EventBatch batch : hec.getBatches()) {
        JsonEventBatch jb = (JsonEventBatch) batch;
        Assert.assertTrue(jb.getEvents().get(0).getFields().containsKey("testField"));
      }

      task.stop();
    }
    
    
    

    private void putWithSuccess(boolean raw, boolean withMeta, Map<String, String> conf) {
        int batchSize = 100;
        int total = 1000;

        UnitUtil uu = new UnitUtil(0);
        Map<String, String> config = uu.createTaskConfig();
        config.put(SplunkSinkConnectorConfig.RAW_CONF, String.valueOf(raw));
        config.put(SplunkSinkConnectorConfig.ACK_CONF, String.valueOf(true));
        config.put(SplunkSinkConnectorConfig.MAX_BATCH_SIZE_CONF, String.valueOf(batchSize));
        config.putAll(conf);
        

        SplunkSinkTask task = new SplunkSinkTask();
        HecMock hec = new HecMock(task);
        // success
        hec.setSendReturnResult(HecMock.success);
        task.setHec(hec);
        task.start(config);
        task.put(createSinkRecords(total));
        Assert.assertEquals(10, hec.getBatches().size());
        if (raw && withMeta) {
            for (EventBatch batch: hec.getBatches()) {
                RawEventBatch rb = (RawEventBatch) batch;
                Assert.assertEquals("i1", rb.getIndex());
                Assert.assertEquals("s1", rb.getSourcetype());
                Assert.assertEquals("e1", rb.getSource());
            }
        }

        // assert data tracking
        if (!raw) {
            int i = 0;;
            for (EventBatch batch: hec.getBatches()) {
                int j = 0;
                for (Event event: batch.getEvents()) {
                    int n = i * 100 + j;
                    Assert.assertEquals(String.valueOf(n), event.getFields().get("kafka_offset"));
                    Assert.assertEquals(String.valueOf(1), event.getFields().get("kafka_partition"));
                    Assert.assertEquals(new UnitUtil(0).configProfile.getTopics(), event.getFields().get("kafka_topic"));
                    Assert.assertEquals(String.valueOf(0), event.getFields().get("kafka_timestamp"));
                    j++;
                }

                i++;
            }
        }

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition(uu.configProfile.getTopics(), 1), new OffsetAndMetadata(1000));
        Assert.assertEquals(offsets, task.preCommit(new HashMap<>()));
        Assert.assertTrue(task.getTracker().getAndRemoveFailedRecords().isEmpty());
        task.stop();
    }
    
    
    
    
    
    
    

    private Collection<SinkRecord> createSinkRecords(int numOfRecords) {
        return createSinkRecords(numOfRecords, 0,"ni, hao");
    }

    private Collection<SinkRecord> createSinkRecords(int numOfRecords, String value) {
        return createSinkRecords(numOfRecords, 0, value);
    }

    private Collection<SinkRecord> createSinkRecords(int numOfRecords, int start, String value) {
        List<SinkRecord> records = new ArrayList<>();
        for (int i = start; i < start + numOfRecords; i++) {
            SinkRecord rec = new SinkRecord(new UnitUtil(0).configProfile.getTopics(), 1, null, null, null, value, i, 0L, TimestampType.NO_TIMESTAMP_TYPE);
            records.add(rec);
        }
        return records;
    }
}
