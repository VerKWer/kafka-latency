package com.spinque.kafka.latency;

public class Params {
  
  public final int msgsPerSec;
  public final int msgSize;  // in bytes
  public final int msgs;
  public final int warmupMsgs;
  public final int cooldownMsgs;
  public final int totalMsgs;
  public final String topic;
  public final short replFactor;
  public final short forcedLeader;
  public final String output;
  
  
  public Params(int msgsPerSec, int secs, int msgSize, String topic, short replicationFactor, short forcedLeader,
      String output) {
    this.msgsPerSec = msgsPerSec;
    this.msgSize = msgSize;
    msgs = secs * msgsPerSec;
    warmupMsgs = 3 * msgsPerSec;
    cooldownMsgs = msgsPerSec;
    totalMsgs = warmupMsgs + msgs + cooldownMsgs;
    this.topic = topic;
    this.replFactor = replicationFactor;
    this.forcedLeader = forcedLeader;
    this.output = output;
  }
  
  
  public static Params parseArgs(String[] args) {
    int msgsPerSec = 200;
    int secs = 10;
    int msgSize = 100;
    String topic = "LatencyTestTopic";
    short replicationFactor = 1;
    short forcedLeader = 0;
    String output = null;
    
    for(String arg : args) {
      if(arg.endsWith("/s"))
        msgsPerSec = Integer.parseInt(arg.substring(0, arg.length() - 2));
      else if(arg.endsWith("s"))
        secs = Integer.parseInt(arg.substring(0, arg.length() - 1));
      else if(arg.endsWith("B"))
        msgSize = Integer.parseInt(arg.substring(0, arg.length() - 1));
      else if(arg.startsWith("topic="))
        topic = arg.substring(6, arg.length());
      else if(arg.startsWith("rf="))
        replicationFactor = Short.parseShort(arg.substring(3, arg.length()));
      else if(arg.startsWith("lead="))
        forcedLeader = Short.parseShort(arg.substring(5, arg.length()));
      else if(arg.endsWith(".png"))
        output = arg;
      else
        System.err.println("Unknown argument: " + arg);
    }
    
    return new Params(msgsPerSec, secs, msgSize, topic, replicationFactor, forcedLeader, output);
  }
  
}
