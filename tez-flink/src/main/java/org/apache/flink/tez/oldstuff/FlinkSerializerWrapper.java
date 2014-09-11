package org.apache.flink.tez.oldstuff;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.io.network.serialization.DataInputDeserializer;
import org.apache.flink.runtime.io.network.serialization.DataOutputSerializer;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class FlinkSerializerWrapper<T> implements Serializer<T> {

    private TypeSerializer<T> serializer;
    private DataOutputView outputView;
    private DataInputView inputView;
    private OutputStream out;

    public FlinkSerializerWrapper(TypeSerializer<T> serializer, DataOutputView outputView, DataInputView inputView) {
        this.serializer = serializer;
        this.outputView = outputView;
        this.inputView = inputView;
    }


    @Override
    public void open(OutputStream out) throws IOException {
        this.out = out;
    }

    @Override
    public void serialize(T t) throws IOException {
        serializer.serialize(t, outputView);
        T reuse = null;
        reuse = serializer.deserialize(reuse, inputView);
        byte [] buf = null;
        inputView.read(buf);
        out.write(buf);
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    public OutputStream getOut() {return out;}


    public static void main (String [] args) {
        StringSerializer stringSer = new StringSerializer();
        DataOutputSerializer outputSer = new DataOutputSerializer(640);
        DataInputDeserializer inputSer = new DataInputDeserializer(new byte[640], 0, 639);
        FlinkSerializerWrapper<String> flinkSer = new FlinkSerializerWrapper<String>(stringSer, outputSer, inputSer);
        OutputStream out = new ByteArrayOutputStream();
        try {
            flinkSer.open(out);
            flinkSer.serialize("Hello");
            System.out.println(flinkSer.getOut().toString());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
