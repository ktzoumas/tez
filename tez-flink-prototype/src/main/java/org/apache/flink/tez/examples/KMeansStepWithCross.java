package org.apache.flink.tez.examples;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichCrossFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.tez.environment.TezExecutionEnvironment;
import org.apache.flink.tez.examples.util.KMeansData;

import java.io.Serializable;
import java.util.Collection;

public class KMeansStepWithCross {


    public static void main(String[] args) throws Exception {

        // set up execution environment
        ExecutionEnvironment env = new TezExecutionEnvironment(true);

        // get input data
        DataSet<Point> points = getPointDataSet(env);
        DataSet<Centroid> centroids = getCentroidDataSet(env);


        /*
        DataSet<Centroid> newCentroids = points
                .cross(centroids)
                .with (new ComputeDistance())



        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(newCentroids, "centroids");

        // emit result
        if(fileOutput) {
            clusteredPoints.writeAsCsv(outputPath, "\n", " ");
        } else {
            clusteredPoints.print();
        }
*/
        // execute program
        env.execute("KMeans Example");

    }

    /**
     * A simple two-dimensional point.
     */
    public static class Point implements Serializable {

        public double x, y;

        public Point() {}

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public Point add(Point other) {
            x += other.x;
            y += other.y;
            return this;
        }

        public Point div(long val) {
            x /= val;
            y /= val;
            return this;
        }

        public double euclideanDistance(Point other) {
            return Math.sqrt((x-other.x)*(x-other.x) + (y-other.y)*(y-other.y));
        }

        public void clear() {
            x = y = 0.0;
        }

        @Override
        public String toString() {
            return x + " " + y;
        }
    }

    /**
     * A simple two-dimensional centroid, basically a point with an ID.
     */
    public static class Centroid extends Point {

        public int id;

        public Centroid() {}

        public Centroid(int id, double x, double y) {
            super(x,y);
            this.id = id;
        }

        public Centroid(int id, Point p) {
            super(p.x, p.y);
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Converts a Tuple2<Double,Double> into a Point. */
    public static final class TuplePointConverter implements MapFunction<Tuple2<Double, Double>, Point> {

        @Override
        public Point map(Tuple2<Double, Double> t) throws Exception {
            return new Point(t.f0, t.f1);
        }
    }

    /** Converts a Tuple3<Integer, Double,Double> into a Centroid. */
    public static final class TupleCentroidConverter implements MapFunction<Tuple3<Integer, Double, Double>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Double, Double> t) throws Exception {
            return new Centroid(t.f0, t.f1, t.f2);
        }
    }

    public static final class ComputeDistance implements CrossFunction<Point,Centroid,Tuple2<Integer,Point>> {
        @Override
        public Tuple2<Integer, Point> cross(Point val1, Centroid val2) throws Exception {
            return null;
        }
    }

    /** Determines the closest cluster center for a data point. */
    public static final class SelectNearestCenter1 extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }

            // emit a new record with the center id and the data point.
            return new Tuple2<Integer, Point>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
            return new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

        @Override
        public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
            return new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

        @Override
        public Centroid map(Tuple3<Integer, Point, Long> value) {
            return new Centroid(value.f0, value.f1.div(value.f2));
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static String pointsPath = null;
    private static String centersPath = null;
    private static String outputPath = null;
    private static int numIterations = 10;

    private static boolean parseParameters(String[] programArguments) {

        if(programArguments.length > 0) {
            // parse input arguments
            fileOutput = true;
            if(programArguments.length == 4) {
                pointsPath = programArguments[0];
                centersPath = programArguments[1];
                outputPath = programArguments[2];
                numIterations = Integer.parseInt(programArguments[3]);
            } else {
                System.err.println("Usage: KMeans <points path> <centers path> <result path> <num iterations>");
                return false;
            }
        } else {
            System.out.println("Executing K-Means example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  We provide a data generator to create synthetic input files for this program.");
            System.out.println("  Usage: KMeans <points path> <centers path> <result path> <num iterations>");
        }
        return true;
    }

    private static DataSet<Point> getPointDataSet(ExecutionEnvironment env) {
        if(fileOutput) {
            // read points from CSV file
            return env.readCsvFile(pointsPath)
                    .fieldDelimiter(' ')
                    .includeFields(true, true)
                    .types(Double.class, Double.class)
                    .map(new TuplePointConverter());
        } else {
            return KMeansData.getDefaultPointDataSet(env);
        }
    }

    private static DataSet<Centroid> getCentroidDataSet(ExecutionEnvironment env) {
        if(fileOutput) {
            return env.readCsvFile(centersPath)
                    .fieldDelimiter(' ')
                    .includeFields(true, true, true)
                    .types(Integer.class, Double.class, Double.class)
                    .map(new TupleCentroidConverter());
        } else {
            return KMeansData.getDefaultCentroidDataSet(env);
        }
    }

}
