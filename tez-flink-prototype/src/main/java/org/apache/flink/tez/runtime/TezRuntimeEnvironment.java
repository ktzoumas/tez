package org.apache.flink.tez.runtime;


import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;

public class TezRuntimeEnvironment {

    private final IOManager ioManager;

    private final MemoryManager memoryManager;

    public TezRuntimeEnvironment(int numPages, int pageSize) {
        this.memoryManager = new DefaultMemoryManager(numPages * pageSize, 10, pageSize);
        this.ioManager = new IOManager();
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }
}
