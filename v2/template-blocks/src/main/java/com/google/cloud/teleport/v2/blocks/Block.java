package com.google.cloud.teleport.v2.blocks;

import com.google.cloud.teleport.v2.blocks.option.Option;

import java.util.List;

public interface Block {
    List<Option> options();
}
