package org.apache.flink.tez.master;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.tez.input.FlinkInputSplitProvider;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import java.util.List;


public class FlinkInputSplitGenerator extends InputInitializer {

    FlinkInputSplitProvider provider;

    public FlinkInputSplitGenerator(InputInitializerContext initializerContext) {
        super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {

                /*
        InputFormat inputFormat = new TextInputFormat(new Path("hellop"));
        InputSplit [] splits = inputFormat.createInputSplits(dop);
        InputSplitAssigner inputSplitAssigner = inputFormat.getInputSplitAssigner(splits);


        InputSplitSource<InputSplit> splitSource = (InputSplitSource<InputSplit>) .getInputSplitSource();
        if (splitSource != null) {
            InputSplit[] splits = splitSource.createInputSplits(numTaskVertices);
            this.splitAssigner = splitSource.getInputSplitAssigner(splits);
        } else {
            this.splitAssigner = null;
        }
        */


        return null;

    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
        for (InputInitializerEvent e: events) {
            InputSplit is = provider.getNextInputSplit();
        }
     }
}
