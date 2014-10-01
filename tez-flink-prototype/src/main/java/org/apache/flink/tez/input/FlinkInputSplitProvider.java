package org.apache.flink.tez.input;


import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.tez.master.FlinkInputSplitGenerator;
import org.apache.flink.tez.plan.FlinkDataSourceVertex;
import org.apache.tez.runtime.api.InputInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class FlinkInputSplitProvider implements InputSplitProvider, Serializable {

    private InputFormat format;
    private int minNumSplits;
    private final List<InputSplit> splits = new ArrayList<InputSplit>();


    public FlinkInputSplitProvider(InputFormat format, int minNumSplits) {
        this.format = format;
        this.minNumSplits = minNumSplits;
        initialize ();
    }

    private void initialize () {
        try {
            InputSplit [] splits = format.createInputSplits(minNumSplits);
            Collections.addAll(this.splits, splits);
        }
        catch (IOException e) {
            throw new RuntimeException("Could initialize InputSplitProvider");
        }

    }


    @Override
    public InputSplit getNextInputSplit() {
        InputSplit next = null;

        // keep the synchronized part short
        synchronized (this.splits) {
            if (this.splits.size() > 0) {
                next = this.splits.remove(this.splits.size() - 1);
            }
        }

        return next;
    }
}
