package com.slack.kaldb.logstore.search;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketches;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.curator.shaded.com.google.common.math.Quantiles.percentiles;

public class DDSektchTest {

  @Test
  public void testDDSketchMultipleMerge() {

    double host1_shard1[] = new double[] {1.1, 1000.77, 56436464.11, 100.4};
    double host1_shard2[] = new double[] {5.5, 555.2, 12334.11, .11};

    double host2_shard1[] = new double[] {.0011, 99, 12334.11, 400};
    double host2_shard2[] = new double[] {111.1, 55, 1, 777777.1};

    double[] all_values =
        new double
            [host1_shard1.length + host1_shard2.length + host2_shard1.length + host2_shard2.length];
    System.arraycopy(host1_shard1, 0, all_values, 0, host1_shard1.length);
    System.arraycopy(host1_shard1, 0, all_values, host1_shard1.length, host1_shard2.length);
    System.arraycopy(host1_shard1, 0, all_values, host1_shard2.length, host2_shard1.length);
    System.arraycopy(host1_shard1, 0, all_values, host2_shard1.length, host2_shard2.length);

    DDSketch sketch_host1_shard1 = DDSketches.unboundedDense( 0.01);
    DDSketch sketch_host1_shard2 = DDSketches.unboundedDense( 0.01);
    DDSketch sketch_host2_shard1 = DDSketches.unboundedDense( 0.01);
    DDSketch sketch_host2_shard2 = DDSketches.unboundedDense( 0.01);

    DDSketch all_sketch_values = DDSketches.unboundedDense( 0.01);

    for (double all_value : all_values) {
      all_sketch_values.accept(all_value);
    }
    for (double value : host1_shard1) {
      sketch_host1_shard1.accept(value);
    }
    for (double value : host1_shard2) {
      sketch_host1_shard2.accept(value);
    }
    for (double value : host2_shard1) {
      sketch_host2_shard1.accept(value);
    }
    for (double value : host2_shard2) {
      sketch_host2_shard2.accept(value);
    }

    System.out.println("Calculating 99th Percentile if all docs were in one shard and no merging");
    System.out.println("Google Math = " + percentiles().index(99).compute(all_values));
    System.out.println("DDSketch    = " + all_sketch_values.getValueAtQuantile(.99));

    System.out.println("DDSketch for host1_shard1 = " + sketch_host1_shard1.getValueAtQuantile(.99));
    System.out.println("DDSketch for host1_shard2 = " + sketch_host1_shard2.getValueAtQuantile(.99));
    System.out.println("DDSketch for host2_shard1 = " + sketch_host2_shard1.getValueAtQuantile(.99));
    System.out.println("DDSketch for host2_shard2 = " + sketch_host2_shard2.getValueAtQuantile(.99));

    DDSketch sink_sketch = DDSketches.unboundedDense( 0.01);

    sink_sketch.mergeWith(sketch_host1_shard1);
    sink_sketch.mergeWith(sketch_host1_shard2);
    sink_sketch.mergeWith(sketch_host2_shard1);
    sink_sketch.mergeWith(sketch_host2_shard2);
    System.out.println("DDSketch for merged data from all individual shards = " + sink_sketch.getValueAtQuantile(.99));

    DDSketch shard1_sketch = DDSketches.unboundedDense( 0.01);
    DDSketch shard2_sketch = DDSketches.unboundedDense( 0.01);
    DDSketch shard1_shard2_sketch = DDSketches.unboundedDense( 0.01);
    shard1_sketch.mergeWith(sketch_host1_shard1);
    shard1_sketch.mergeWith(sketch_host1_shard2);

    shard2_sketch.mergeWith(sketch_host2_shard1);
    shard2_sketch.mergeWith(sketch_host2_shard2);

    shard1_shard2_sketch.mergeWith(shard1_sketch);
    shard1_shard2_sketch.mergeWith(shard2_sketch);

    System.out.println("DDSketch for merged data with 2 level aggregation = " + shard1_shard2_sketch.getValueAtQuantile(.99));
  }
}
