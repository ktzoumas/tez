package org.apache.flink.tez.runtime;


import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

public class DummyInvokable extends AbstractInvokable {

    @Override
    public void registerInputOutput() {}


    @Override
    public void invoke() throws Exception {}
}
