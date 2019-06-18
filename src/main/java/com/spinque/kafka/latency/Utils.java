package com.spinque.kafka.latency;

public class Utils {
  
  public static byte[] longToBytes(long n) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; --i) {
        result[i] = (byte)n;
        n >>>= 8;
    }
    return result;
  }
  
  public static long bytesToLong(byte[] bs) {
    long result = 0;
    for (int i = 0; i < 8; ++i) {
        result <<= 8;
        result |= (bs[i] & 0xff);
    }
    return result;
  }
  
}
