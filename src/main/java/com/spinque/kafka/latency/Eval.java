package com.spinque.kafka.latency;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Eval {
  
  public static String evalTimes(int leaderID, Params params, List<Long> times) {
    List<Long> sorted = new ArrayList<>(times);
    Collections.sort(sorted);
    long avg = 0;
    for(long time : sorted) avg += time;
    avg /= sorted.size();
    StringBuilder sb = new StringBuilder();
    sb.append("Latency measurements at ").append(params.msgsPerSec).append("msgs/s of size ")
      .append(params.msgSize).append("B:\n");
    sb.append("Replication factor: ").append(params.replFactor).append(", LeaderID: ").append(leaderID).append("\n");
    sb.append("Warmup: ").append(params.warmupMsgs).append("msgs, Timed: ").append(params.msgs).append("msgs, Cooldown: ")
        .append(params.cooldownMsgs).append("msgs\n");
    sb.append("Average: ").append(avg/1000).append("μs\n");
    sb.append("Median: " ).append(sorted.get(sorted.size()/2)/1000).append("μs\n");
    sb.append("90% Percentile: ").append(sorted.get(sorted.size() * 9/10)/1000).append("μs\n");
    sb.append("95% Percentile: ").append(sorted.get(sorted.size() * 95/100)/1000).append("μs\n");
    sb.append("99% Percentile: ").append(sorted.get(sorted.size() * 99/100)/1000).append("μs\n");
    sb.append("Worst: ").append(sorted.get(sorted.size() - 1)/1000).append("μs");
    return sb.toString();
  }
  
  public static void plot(String legend, List<Long> sendTimes, List<Long> latencies, String output) throws IOException {
    try {
      File tmpFile = File.createTempFile("kafkaLatencyTest", ".data");
      tmpFile.deleteOnExit();
      BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
      for(int i = 0; i < latencies.size(); ++i) {
        long sendTime = sendTimes.get(i) - sendTimes.get(0);
        long latency = latencies.get(i);
        writer.write(String.valueOf(sendTime));
        writer.write(" ");
        writer.write(String.valueOf(latency/1000));
        writer.newLine();
      }
      writer.close();
      
      Collections.sort(latencies);
      File tmpFile2 = File.createTempFile("kafkaLatencyTestSorted", ".data");
      tmpFile2.deleteOnExit();
      writer = new BufferedWriter(new FileWriter(tmpFile2));
      for(long latency : latencies) {
        writer.write(String.valueOf(latency/1000));
        writer.newLine();
      }
      writer.close();
      
      legend = legend.replaceAll("\n", "\\\\n");
      File gpFile = File.createTempFile("kafkaLatencyTest", ".gp");
      gpFile.deleteOnExit();
      long max = latencies.get(latencies.size() - 1)/1000;
      
      if(output != null) {
        writer = new BufferedWriter(new FileWriter(gpFile));
        writer.write("set terminal png size 1500, 700\n");
        writer.write("set output \"" + output + "\"\n");
        writer.write("set encoding utf8\n");
        writer.write("set yrange [0:" + (max + max/20) + "]\n");
        writer.write("set ylabel \"Latency (μs)\"\n");
        writer.write("set grid\n");
        writer.write("set multiplot layout 1,2 margins .05,.95,.05,.95 spacing .05\n");
        writer.write("set title \"Chronological\" font \",14\"\n");
        writer.write("set xlabel \"Time (ms)\"\n");
        writer.write("plot \"" + tmpFile.getAbsolutePath() + "\" with lines notitle\n");
        writer.write("set title \"Sorted\" font \",14\"\n");
        writer.write("set label \"");
        writer.write(legend);
        writer.write("\" at " + latencies.size()/20 + ", " + max + "\n");
        writer.write("unset xtics\n");
        writer.write("unset xlabel\n");
        writer.write("plot \"" + tmpFile2.getAbsolutePath() + "\" with lines notitle\n");
        writer.close();
        Runtime.getRuntime().exec(new String[] { "gnuplot", gpFile.getAbsolutePath() }).waitFor();
      }

      writer = new BufferedWriter(new FileWriter(gpFile));
      writer.write("set terminal x11 size 1500, 700\n");
      writer.write("set yrange [0:" + (max + max/20) + "]\n");
      writer.write("set ylabel \"Latency (us)\"\n");
      writer.write("set grid\n");
      writer.write("set multiplot layout 1,2 margins .05,.95,.05,.95 spacing .05\n");
      writer.write("set title \"Chronological\" font \",14\"\n");
      writer.write("set xlabel \"Time (ms)\"\n");
      writer.write("plot \"" + tmpFile.getAbsolutePath() + "\" with lines notitle\n");
      writer.write("set title \"Sorted\" font \",14\"\n");
      writer.write("set label \"");
      writer.write(legend.replaceAll("μ", "u"));
      writer.write("\" at " + latencies.size()/20 + ", " + max + "\n");
      writer.write("unset xtics\n");
      writer.write("unset xlabel\n");
      writer.write("plot \"" + tmpFile2.getAbsolutePath() + "\" with lines notitle\n");
      writer.close();
      // Note: Need .waitFor() because otherwise, the temporary files will be deleted too quickly.
      Runtime.getRuntime().exec(new String[] { "gnuplot",  "-p", gpFile.getAbsolutePath() }).waitFor();
    } catch(InterruptedException e) { }
  }
  
}
