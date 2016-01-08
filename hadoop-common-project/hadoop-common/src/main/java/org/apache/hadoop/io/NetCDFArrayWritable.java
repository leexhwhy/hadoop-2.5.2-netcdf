package org.apache.hadoop.io;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;

import java.util.Comparator;

public class NetCDFArrayWritable extends ArrayWritable implements Comparable<NetCDFArrayWritable> {
  public NetCDFArrayWritable() {
    super(FloatWritable.class); 
  }


  @Override
  public int compareTo(NetCDFArrayWritable o) {

    FloatWritable thisLat = (FloatWritable)(this.get()[0]);
    FloatWritable thisTime = (FloatWritable)(this.get()[1]);

    FloatWritable thatLat = (FloatWritable)(o.get()[0]);
    FloatWritable thatTime = (FloatWritable)(o.get()[1]);

    if( thisLat.get() > thatLat.get() )
      return 1;
    else if( thisLat.get() < thatLat.get() )
      return -1;
    else{
      if( thisTime.get() > thatTime.get() )
        return 1;
      else if( thisTime.get() < thatTime.get() )
        return -1;
      else
        return 0;
    }

  }
}
