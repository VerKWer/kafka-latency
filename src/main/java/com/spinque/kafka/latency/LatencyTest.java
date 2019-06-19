package com.spinque.kafka.latency;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class LatencyTest {
  
  private String bootstrapServers;
  private final Params params;
  private final Map<String, String> topicConfig = new HashMap<>();
  private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1);
  private final List<Long> sendTimes;
  private final List<Long> latencies;
  private final byte[] payload;  // so we don't always have to reallocate a new array of zeroes
  private AdminClient admin;
  private volatile KafkaConsumer<byte[], byte[]> consumer;
  private KafkaProducer<byte[], byte[]> producer;
  
  
  public LatencyTest(Params params) {
    this.params = params;
    sendTimes = new ArrayList<>(params.msgs);
    latencies = new ArrayList<>(params.msgs);
    topicConfig.put("message.timestamp.type", "CreateTime");
    topicConfig.put("retention.bytes",
        String.valueOf(Math.min((long)params.totalMsgs * params.msgSize * 2, Integer.MAX_VALUE)));
    topicConfig.put("retention.ms", String.valueOf(params.totalMsgs/params.msgsPerSec * 2000));
    topicConfig.put("segment.bytes",
        String.valueOf(Math.min((long)params.totalMsgs * params.msgSize * 2, Integer.MAX_VALUE)));
    topicConfig.put("segment.ms", String.valueOf(params.totalMsgs/params.msgsPerSec * 2000));
    payload = new byte[params.msgSize];
  }
  
  
  private void initBootstrapServers() {
    String bootstrap = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    if(bootstrap == null)
      try {
        bootstrap = InetAddress.getLocalHost().getHostName() + ":9092";
      } catch(UnknownHostException e1) {
        bootstrap = "localhost:9092";
      }
    bootstrapServers = bootstrap;
  }
  
  private void initAdmin() {
    Properties config = new Properties();
    config.put("bootstrap.servers", bootstrapServers);
    admin = AdminClient.create(config);
  }
  
  private void initConsumer() {
    Properties config = new Properties();
    config.put("client.id", "TestClient");
    config.put("bootstrap.servers", bootstrapServers);
    config.put("enable.auto.commit", "false");
    consumer = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    consumer.assign(Collections.singleton(new TopicPartition(params.topic, 0)));
    consumer.seekToEnd(Collections.singleton(new TopicPartition(params.topic, 0)));
  }
  
  private int initTopic(int tries) {
    if(tries < 10) {
      System.out.println("Resetting topic " + params.topic);
      try {
        admin.deleteTopics(Collections.singleton(params.topic)).all().get();
      } catch(InterruptedException | ExecutionException e) {
        if(!(e.getCause() instanceof UnknownTopicOrPartitionException))
          throw new RuntimeException(e);
      }
      try { Thread.sleep(1000); } catch(InterruptedException e) { }
      NewTopic nt = new NewTopic(params.topic, 1, params.replFactor).configs(topicConfig);
      try {
        admin.createTopics(Collections.singleton(nt)).all().get();
      } catch(InterruptedException | ExecutionException e) {
        throw new RuntimeException("Couldn't create topic " + params.topic, e);
      }
      try { Thread.sleep(1000); } catch(InterruptedException e) { }
      try {
        int leaderID =
            admin.describeTopics(Collections.singleton(params.topic)).all().get().get(params.topic).partitions().get(0)
                 .leader().id();
        if((params.forcedLeader < 0 && leaderID == 0) ||
           (params.forcedLeader >= 0 && leaderID != params.forcedLeader)) {
          System.out.println("Wrong leader. Retrying...");
          return initTopic(tries + 1);
        } else {
          return leaderID;
        }
      } catch(InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    } else {
      throw new RuntimeException("Didn't succeed in forcing leader after 10 tries. Giving up!");
    }
  }
  
  private void initProducer() {
    Properties config = new Properties();
    config.put("bootstrap.servers", bootstrapServers);
    config.put("acks", "0");
    producer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
  }
  
  private Thread consumerThread = new Thread(new Runnable() {
    public void run() {
      System.out.println("Starting consumer thread");
      try {
        int noMsgs = 0;
        while(noMsgs < params.totalMsgs && !Thread.currentThread().isInterrupted()) {
          ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
          try {
            records = consumer.poll(Duration.ofMillis(100));
            long now = System.nanoTime();
            for(ConsumerRecord<byte[], byte[]> record : records) {
              if(noMsgs >= params.warmupMsgs && noMsgs < params.warmupMsgs + params.msgs) {
                long diff = now - Utils.bytesToLong(record.value());
                sendTimes.add(record.timestamp());
                latencies.add(diff);
              }
              ++noMsgs;
            }
          } catch(InterruptException e) {  // unchecked and thrown by poll (OBS: doesn't clear interrupt flag!)
            throw new InterruptedException(e.getMessage());
          }
        }
      } catch(InterruptedException e) {
      } finally {
        consumer.close();
        System.out.println("Consumer thread stopped");
      }
    }
  });
  
  private void sendMsg(AtomicInteger msgsSent) {
    int sent = msgsSent.getAndIncrement();
    if(sent < params.totalMsgs) {
      long now = System.nanoTime();
      byte[] nowBytes = Utils.longToBytes(now);
      for(int i = 0; i < 8; ++i)
        payload[i] = nowBytes[i];
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(params.topic, payload);
      
      if(sent == 0)
        System.out.println("Warming up...");
      else if(sent == params.warmupMsgs)
          System.out.println("Warmup done! Timing...");
      else if(sent == params.warmupMsgs + params.msgs)
        System.out.println("Timing done! Cooling down...");
      else if(sent == params.totalMsgs)
        System.out.println("All messages sent!");
      producer.send(record);
    }
  }
  
  
  public static void main(String[] args) {
    System.out.println("Starting Kafka latency test...");
    Params params = Params.parseArgs(args);
    LatencyTest lt = new LatencyTest(params);
    lt.initBootstrapServers();
    lt.initAdmin();
    int leaderID = lt.initTopic(0);
    lt.admin.close();
    lt.initConsumer();
    lt.initProducer();
    lt.consumerThread.start();
    try { Thread.sleep(100); } catch(InterruptedException e) { }
    
    System.out.println("Sending messages...");
    AtomicInteger msgsSent = new AtomicInteger();
    lt.executor.scheduleAtFixedRate(()->lt.sendMsg(msgsSent), 0, 1000000l/lt.params.msgsPerSec, TimeUnit.MICROSECONDS);
    try { lt.consumerThread.join(); } catch(InterruptedException e) { }
    lt.executor.shutdownNow();
    lt.producer.close();
    System.out.println("Evaluating results...\n");
    String legend = Eval.evalTimes(leaderID, params, lt.latencies);
    System.out.println(legend);
    try {
      Eval.plot(legend, lt.sendTimes, lt.latencies, params.output);
    } catch(IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
