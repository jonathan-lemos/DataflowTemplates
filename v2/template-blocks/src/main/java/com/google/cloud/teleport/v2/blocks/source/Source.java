package com.google.cloud.teleport.v2.blocks.source;

import com.google.cloud.teleport.v2.blocks.Block;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

public interface Source<OutputT> extends Block {
    PCollection<OutputT> readFromSource(Pipeline p);
}
