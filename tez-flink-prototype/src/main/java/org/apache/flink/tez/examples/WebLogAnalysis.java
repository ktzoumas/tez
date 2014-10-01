package org.apache.flink.tez.examples;


import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.tez.environment.TezExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WebLogAnalysis {


    private static String documentsPath = "/tmp/documents";
    private static String ranksPath = "/tmp/ranks";
    private static String visitsPath = "/tmp/visits";
    private static String outputPath = "/tmp/weblog_out9";

    public static void main (String [] args) throws Exception {
        final ExecutionEnvironment env = new TezExecutionEnvironment(true);
        env.setDegreeOfParallelism(4);

        // get input data
        DataSet<Tuple2<String, String>> documents = getDocumentsDataSet(env);
        DataSet<Tuple3<Integer, String, Integer>> ranks = getRanksDataSet(env);
        DataSet<Tuple2<String, String>> visits = getVisitsDataSet(env);

        // Retain documents with keywords
        DataSet<Tuple1<String>> filterDocs = documents
                .filter(new FilterDocByKeyWords())
                .project(0).types(String.class);

        // Filter ranks by minimum rank
        DataSet<Tuple3<Integer, String, Integer>> filterRanks = ranks
                .filter(new FilterByRank());

        // Filter visits by visit date
        DataSet<Tuple1<String>> filterVisits = visits
                .filter(new FilterVisitsByDate())
                .project(0).types(String.class);

        // Join the filtered documents and ranks, i.e., get all URLs with min rank and keywords
        DataSet<Tuple3<Integer, String, Integer>> joinDocsRanks =
                filterDocs.join(filterRanks)
                        .where(0).equalTo(1)
                        .projectSecond(0,1,2)
                        .types(Integer.class, String.class, Integer.class);

        // Anti-join urls with visits, i.e., retain all URLs which have NOT been visited in a certain time
        DataSet<Tuple3<Integer, String, Integer>> result =
                joinDocsRanks.coGroup(filterVisits)
                        .where(1).equalTo(0)
                        .with(new AntiJoinVisits());

        joinDocsRanks.writeAsCsv(outputPath, "\n", "|");

        env.execute();
    }


    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * MapFunction that filters for documents that contain a certain set of
     * keywords.
     */
    public static class FilterDocByKeyWords implements FilterFunction<Tuple2<String, String>> {

        private static final String[] KEYWORDS = { " editors ", " oscillations " };

        /**
         * Filters for documents that contain all of the given keywords and projects the records on the URL field.
         *
         * Output Format:
         * 0: URL
         * 1: DOCUMENT_TEXT
         */
        @Override
        public boolean filter(Tuple2<String, String> value) throws Exception {
            // FILTER
            // Only collect the document if all keywords are contained
            String docText = value.f1;
            for (String kw : KEYWORDS) {
                if (!docText.contains(kw)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * MapFunction that filters for records where the rank exceeds a certain threshold.
     */
    public static class FilterByRank implements FilterFunction<Tuple3<Integer, String, Integer>> {

        private static final int RANKFILTER = 40;

        /**
         * Filters for records of the rank relation where the rank is greater
         * than the given threshold.
         *
         * Output Format:
         * 0: RANK
         * 1: URL
         * 2: AVG_DURATION
         */
        @Override
        public boolean filter(Tuple3<Integer, String, Integer> value) throws Exception {
            return (value.f0 > RANKFILTER);
        }
    }

    /**
     * MapFunction that filters for records of the visits relation where the year
     * (from the date string) is equal to a certain value.
     */
    public static class FilterVisitsByDate implements FilterFunction<Tuple2<String, String>> {

        private static final int YEARFILTER = 2007;

        /**
         * Filters for records of the visits relation where the year of visit is equal to a
         * specified value. The URL of all visit records passing the filter is emitted.
         *
         * Output Format:
         * 0: URL
         * 1: DATE
         */
        @Override
        public boolean filter(Tuple2<String, String> value) throws Exception {
            // Parse date string with the format YYYY-MM-DD and extract the year
            String dateString = value.f1;
            int year = Integer.parseInt(dateString.substring(0,4));
            return (year == YEARFILTER);
        }
    }


    /**
     * CoGroupFunction that realizes an anti-join.
     * If the first input does not provide any pairs, all pairs of the second input are emitted.
     * Otherwise, no pair is emitted.
     */
    public static class AntiJoinVisits implements CoGroupFunction<Tuple3<Integer, String, Integer>, Tuple1<String>, Tuple3<Integer, String, Integer>> {

        /**
         * If the visit iterator is empty, all pairs of the rank iterator are emitted.
         * Otherwise, no pair is emitted.
         *
         * Output Format:
         * 0: RANK
         * 1: URL
         * 2: AVG_DURATION
         */
        @Override
        public void coGroup(Iterable<Tuple3<Integer, String, Integer>> ranks, Iterable<Tuple1<String>> visits, Collector<Tuple3<Integer, String, Integer>> out) {
            // Check if there is a entry in the visits relation
            if (!visits.iterator().hasNext()) {
                for (Tuple3<Integer, String, Integer> next : ranks) {
                    // Emit all rank pairs
                    out.collect(next);
                }
            }
        }
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************


    private static DataSet<Tuple2<String, String>> getDocumentsDataSet(ExecutionEnvironment env) {
        // Create DataSet for documents relation (URL, Doc-Text)
        return env.readCsvFile(documentsPath)
                .fieldDelimiter('|')
                .types(String.class, String.class);
    }

    private static DataSet<Tuple3<Integer, String, Integer>> getRanksDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(ranksPath)
                .fieldDelimiter('|')
                .types(Integer.class, String.class, Integer.class);
    }

    private static DataSet<Tuple2<String, String>> getVisitsDataSet(ExecutionEnvironment env) {
        return env.readCsvFile(visitsPath)
                .fieldDelimiter('|')
                .includeFields("011000000")
                .types(String.class, String.class);
    }

}
