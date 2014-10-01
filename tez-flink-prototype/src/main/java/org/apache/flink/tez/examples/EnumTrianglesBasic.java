package org.apache.flink.tez.examples;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.tez.environment.TezExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.tez.examples.util.EnumTrianglesData;
import org.apache.flink.tez.examples.util.EnumTrianglesDataTypes.Edge;
import org.apache.flink.tez.examples.util.EnumTrianglesDataTypes.Triad;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EnumTrianglesBasic {

    static boolean fileOutput = false;
    static String edgePath = null;
    static String outputPath = null;

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        // set up execution environment
        final ExecutionEnvironment env = new TezExecutionEnvironment(true);

        // read input data
        DataSet<Edge> edges = getEdgeDataSet(env);

        // project edges by vertex id
        DataSet<Edge> edgesById = edges
                .map(new EdgeByIdProjector());

        DataSet<Triad> triangles = edgesById
                // build triads
                .groupBy(Edge.V1).sortGroup(Edge.V2, Order.ASCENDING).reduceGroup(new TriadBuilder())
                        // filter triads
                .join(edgesById).where(Triad.V2, Triad.V3).equalTo(Edge.V1, Edge.V2).with(new TriadFilter());

        // emit result
        if(fileOutput) {
            triangles.writeAsCsv(outputPath, "\n", ",");
        } else {
            triangles.print();
        }

        // execute program
        env.execute("Basic Triangle Enumeration Example");

    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Converts a Tuple2 into an Edge */
    public static class TupleEdgeConverter implements MapFunction<Tuple2<Integer, Integer>, Edge> {
        private final Edge outEdge = new Edge();

        @Override
        public Edge map(Tuple2<Integer, Integer> t) throws Exception {
            outEdge.copyVerticesFromTuple2(t);
            return outEdge;
        }
    }

    /** Projects an edge (pair of vertices) such that the id of the first is smaller than the id of the second. */
    private static class EdgeByIdProjector implements MapFunction<Edge, Edge> {

        @Override
        public Edge map(Edge inEdge) throws Exception {

            // flip vertices if necessary
            if(inEdge.getFirstVertex() > inEdge.getSecondVertex()) {
                inEdge.flipVertices();
            }

            return inEdge;
        }
    }

    /**
     *  Builds triads (triples of vertices) from pairs of edges that share a vertex.
     *  The first vertex of a triad is the shared vertex, the second and third vertex are ordered by vertexId.
     *  Assumes that input edges share the first vertex and are in ascending order of the second vertex.
     */
    private static class TriadBuilder implements GroupReduceFunction<Edge, Triad> {
        private final List<Integer> vertices = new ArrayList<Integer>();
        private final Triad outTriad = new Triad();

        @Override
        public void reduce(Iterable<Edge> edgesIter, Collector<Triad> out) throws Exception {

            final Iterator<Edge> edges = edgesIter.iterator();

            // clear vertex list
            vertices.clear();

            // read first edge
            Edge firstEdge = edges.next();
            outTriad.setFirstVertex(firstEdge.getFirstVertex());
            vertices.add(firstEdge.getSecondVertex());

            // build and emit triads
            while (edges.hasNext()) {
                Integer higherVertexId = edges.next().getSecondVertex();

                // combine vertex with all previously read vertices
                for (Integer lowerVertexId : vertices) {
                    outTriad.setSecondVertex(lowerVertexId);
                    outTriad.setThirdVertex(higherVertexId);
                    out.collect(outTriad);
                }
                vertices.add(higherVertexId);
            }
        }
    }

    /** Filters triads (three vertices connected by two edges) without a closing third edge. */
    private static class TriadFilter implements JoinFunction<Triad, Edge, Triad> {

        @Override
        public Triad join(Triad triad, Edge edge) throws Exception {
            return triad;
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean parseParameters(String[] args) {

        if(args.length > 0) {
            // parse input arguments
            fileOutput = true;
            if(args.length == 2) {
                edgePath = args[0];
                outputPath = args[1];
            } else {
                System.err.println("Usage: EnumTriangleBasic <edge path> <result path>");
                return false;
            }
        } else {
            System.out.println("Executing Enum Triangles Basic example with built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: EnumTriangleBasic <edge path> <result path>");
        }
        return true;
    }

    private static DataSet<Edge> getEdgeDataSet(ExecutionEnvironment env) {
        if(fileOutput) {
            return env.readCsvFile(edgePath)
                    .fieldDelimiter(' ')
                    .includeFields(true, true)
                    .types(Integer.class, Integer.class)
                    .map(new TupleEdgeConverter());
        } else {
            return EnumTrianglesData.getDefaultEdgeDataSet(env);
        }
    }

}

