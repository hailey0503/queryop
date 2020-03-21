package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        // TODO(proj3_part1): implement
        // just get a record iterator over the input run and add the records into your run.
        //Take a look at the functions in the Run interface.

        //Iterator<Record> iter = iterator();
        List<Record> records = createRecordList(run);
        Collections.sort(records, comparator);
        //PriorityQueue<Comparator> pq = new PriorityQueue(comparator);
        Run newRun = createRun();
        newRun.addRecords(records);
        return newRun;
    }
    private List<Record> createRecordList(Run run) {
        List<Record> records = new ArrayList<>();
        run.iterator().forEachRemaining(r-> records.add(r));
        return records;
    }

    /**
     * Given a list of sorted runs, returns a NEW run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        // TODO(proj3_part1): implement
        RecordPairComparator comp = new RecordPairComparator();
        Run newRun = createRun();
        Queue<Pair<Record, Integer>> pq = new PriorityQueue<>(comp);
        //int input_buffer = numBuffers - 1;
        //To save a list of iterators before do anything and use those instead
        List<Iterator<Record>> iter_list = new ArrayList<>();
        //add each run's first record to PQ
        addFirst(runs, iter_list, pq);
        while (!pq.isEmpty()) {
            Pair<Record, Integer> nextPair = pq.poll();
            Record rec = nextPair.getFirst();
            int intVal = nextPair.getSecond();
            newRun.addRecord(rec.getValues());
            if (iter_list.get(intVal).hasNext()) {
                pq.add(new Pair<>(iter_list.get(intVal).next(), intVal));
            }
        }

        return newRun;
    }
    private void addFirst(List<Run> runs, List<Iterator<Record>> iter_list,
                          Queue<Pair<Record, Integer>> pq) {
        for (int i = 0; i < runs.size(); i++) {
            Iterator<Record> iter = runs.get(i).iterator();
            iter_list.add(i, iter);
            if (iter_list.get(i).hasNext()) {
                Pair<Record, Integer> newPair = new Pair<>(iter_list.get(i).next(), i);
                pq.add(newPair);
            }
        }
    }
    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time. It is okay for the last sorted run
     * to use less than (numBuffers - 1) input runs if N is not a
     * perfect multiple.
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        //takes a list of N sorted runs and merges them into ceil(N / (B - 1)) sorted runs
        // if we had 20 pages of records and 7 buffer pages,
        // then the first 6 pages would get merged into a run, then the next 6 pages,
        // the next 6, and then the final 2.
        // You just merge them in the order they happen to be in in the list that's passed in.
        List<Run> mergedRun = new ArrayList<>();
        for (int i = 0; i < runs.size(); i += numBuffers - 1) {
            List<Run> batch = runs.subList(i, i + numBuffers - 1);
            mergedRun.add(mergeSortedRuns(batch));
        }

        return mergedRun;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        // TODO(proj3_part1): implement
        //this.transaction.getPageIterator <- block we pass into getBlockIterator
        //what is going on in pass 0:
        //If there are 5*B pages total (B=#of buffers), then after pass 0
        // I should have 5 separate, but sorted runs stored in memory? YES
        //getBlockIterator returns an iterator that is perfectly valid to pass into createRunFromIterator.
        //If you use getBlockIterator appropriately, you should be able to create the separate iterators you are looking for.
        //Don't call pageIter.next(). getPageIterator already takes care of the header page.
        // getBlockIterator advances the page iterator forward.
        //You may find createRunFromIterator useful as an alternative that avoids some I/Os.
        List<Run> runList = new ArrayList<>();
        BacktrackingIterator<Page> pageIter = transaction.getPageIterator(tableName);
        while (pageIter.hasNext()) {
            BacktrackingIterator<Record> blockIter =
                    transaction.getBlockIterator(tableName, pageIter, numBuffers);//# of page for pass 0??
            Run pass0 = createRunFromIterator(blockIter);
            Run sortedRun = sortRun(pass0);
            runList.add(sortedRun);
        }
        while (runList.size() > 1) {
            runList = mergePass(runList);
        }
        String tableName = runList.get(0).tableName();
        return tableName;
    }



    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                                     SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

