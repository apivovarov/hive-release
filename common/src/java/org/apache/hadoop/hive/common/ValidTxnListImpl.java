package org.apache.hadoop.hive.common;

import java.util.Arrays;

public class ValidTxnListImpl implements ValidTxnList {

  private long[] exceptions;
  private long highWatermark;

  public ValidTxnListImpl() {
    this(new long[0], Long.MAX_VALUE);
  }

  public ValidTxnListImpl(long[] exceptions, long highWatermark) {
    if (exceptions.length == 0) {
      this.exceptions = exceptions;
    } else {
      this.exceptions = exceptions.clone();
      Arrays.sort(this.exceptions);
    }
    this.highWatermark = highWatermark;
  }

  public ValidTxnListImpl(String value) {
    fromString(value);
  }

  @Override
  public boolean isTxnCommitted(long txnid) {
    if (highWatermark < txnid) {
      return false;
    }
    for(long except: exceptions) {
      if (txnid == except) {
        return false;
      } else if (txnid < except) {
        // we can short circuit, because the exceptions are sorted
        return true;
      }
    }
    return true;
  }

  @Override
  public RangeResponse isTxnRangeCommitted(long minTxnId, long maxTxnId) {
    // check the easy cases first
    if (highWatermark < minTxnId) {
      return RangeResponse.NONE;
    } else if (exceptions.length > 0 && exceptions[0] > maxTxnId) {
      return RangeResponse.ALL;
    }

    // since the exceptions and the range in question overlap, count the
    // exceptions in the range
    long count = Math.max(0, maxTxnId - highWatermark);
    for(long txn: exceptions) {
      if (minTxnId <= txn && txn <= maxTxnId) {
        count += 1;
      }
    }

    if (count == 0) {
      if (highWatermark >= maxTxnId) {
        return RangeResponse.ALL;
      }
    } else if (count == (maxTxnId - minTxnId + 1)) {
      return RangeResponse.NONE;
    }
    return RangeResponse.SOME;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(highWatermark);
    if (exceptions.length == 0) {
      buf.append(':');
    } else {
      for(long except: exceptions) {
        buf.append(':');
        buf.append(except);
      }
    }
    return buf.toString();
  }

  @Override
  public void fromString(String src) {
    if (src == null) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new long[0];
    } else {
      String[] values = src.split(":");
      highWatermark = Long.parseLong(values[0]);
      exceptions = new long[values.length - 1];
      for(int i = 1; i < values.length; ++i) {
        exceptions[i-1] = Long.parseLong(values[i]);
      }
    }
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
  public long[] getOpenTransactions() {
    return exceptions;
  }
}

