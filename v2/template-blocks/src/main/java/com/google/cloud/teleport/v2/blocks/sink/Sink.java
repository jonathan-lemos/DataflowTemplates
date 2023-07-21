package com.google.cloud.teleport.v2.blocks.sink;

import com.google.cloud.teleport.v2.blocks.Block;
import org.apache.beam.sdk.values.PCollection;

public interface Sink<InputT> extends Block {
    void writeToSink(PCollection<InputT> input);
}
