package com.google.cloud.teleport.v2.blocks.option;

import com.google.auto.value.AutoValue;

import java.util.List;

@AutoValue
public abstract class Option {
        /** Name of the parameter. */
        public abstract String getCanonicalName();

        /** Aliases of the parameter, if any. */
        public abstract List<String> getAliases();

        /** If parameter is optional. */
        public abstract boolean getOptional();

        /** Description of the parameter. */
        public abstract String getDescription();

        /** Help text of the parameter. */
        public abstract String getHelpText();

        /** Example of the parameter. */
        public abstract String getExample();
}
