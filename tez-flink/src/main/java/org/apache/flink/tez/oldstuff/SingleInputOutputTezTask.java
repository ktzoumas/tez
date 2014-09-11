package org.apache.flink.tez.oldstuff;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.util.ReaderIterator;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.tez.wordcount.MutableKeyValueReader;
import org.apache.flink.tez.wordcount.TezTaskContext;
import org.apache.flink.util.Collector;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.processor.SimpleProcessor;


public class SingleInputOutputTezTask<F extends Function, IT, OT> extends SimpleProcessor {

    private F function;
    private TezTaskContext<F, OT> flinkContext;
    private TypeSerializer<IT> inputTypeSerializer;
    private PactDriver<F, OT> driver;

    public SingleInputOutputTezTask(ProcessorContext context, F function, PactDriver<F, OT> driver, TypeSerializer<IT> inputTypeSerializer, Collector<OT> collector) {
        super(context);
        this.function = function;
        this.inputTypeSerializer = inputTypeSerializer;
        this.flinkContext = new TezTaskContext<F, OT>();
        this.flinkContext.setUdf(this.function);
        this.flinkContext.setCollector(collector);
        this.driver = driver;
    }

    @Override
    public void run() throws Exception {
        KeyValueReader kvReader = (KeyValueReader) getInputs().values().iterator().next().getReader();
        ReaderIterator<IT> iterator = new ReaderIterator<IT>(new MutableKeyValueReader<DeserializationDelegate<IT>>(kvReader), inputTypeSerializer);
        flinkContext.setInput1(iterator, inputTypeSerializer);
        driver.setup(flinkContext);
        driver.prepare();
        driver.run();
    }
}
