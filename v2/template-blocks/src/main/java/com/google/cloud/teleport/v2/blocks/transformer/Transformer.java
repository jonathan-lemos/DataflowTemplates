package com.google.cloud.teleport.v2.blocks.transformer;

import com.google.cloud.teleport.v2.blocks.Block;
import org.apache.beam.sdk.values.PCollection;

public interface Transformer<InputT, OutputT> extends Block {
    PCollection<OutputT> transform(PCollection<InputT> input);
}
