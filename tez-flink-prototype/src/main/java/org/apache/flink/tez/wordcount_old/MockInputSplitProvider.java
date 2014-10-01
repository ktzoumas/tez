package org.apache.flink.tez.wordcount_old;


import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;


public class MockInputSplitProvider implements InputSplitProvider, Serializable {

    /**
     * The input splits to be served during the test.
     */
    private volatile InputSplit[] inputSplits;

    /**
     * Index pointing to the next input split to be served.
     */
    private int nextSplit = 0;

    /**
     * Generates a set of input splits from an input path
     *
     * @param path
     *        the path of the local file to generate the input splits from
     * @param noSplits
     *        the number of input splits to be generated from the given input file
     */
    public void addInputSplits(final String path, final int noSplits) {

        final InputSplit[] tmp = new InputSplit[noSplits];
        final String[] hosts = { "localhost" };

        final String localPath;
        try {
            localPath = new URI(path).getPath();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Path URI can not be transformed to local path.");
        }

        final File inFile = new File(localPath);

        final long splitLength = inFile.length() / noSplits;
        long pos = 0;

        for (int i = 0; i < noSplits - 1; i++) {
            tmp[i] = new FileInputSplit(i, new Path(path), pos, splitLength, hosts);
            pos += splitLength;
        }

        tmp[noSplits - 1] = new FileInputSplit(noSplits - 1, new Path(path), pos, inFile.length() - pos, hosts);

        this.inputSplits = tmp;
    }


    @Override
    public InputSplit getNextInputSplit() {

        if (this.nextSplit < this.inputSplits.length) {
            return this.inputSplits[this.nextSplit++];
        }

        return null;
    }

}


