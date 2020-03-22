package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * See lecture slides.
     * <p>
     * Before proceeding, you should read and understand SNLJOperator.java
     * You can find it in the same directory as this file.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given (Once again,
     * SNLJOperator.java might be a useful reference).
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        //You have two comparators (one left and one right). You also have two tables (one left and one right).
        //You can create a SortOperator object for each table, but the constructor needs a comparator.
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;
        private RecordComparator sortComparator;
        private SortMergeIterator() {
            super();
            // TODO(proj3_part1): implement
            SortOperator leftSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(),
                    new LeftRecordComparator());
            SortOperator rightSortOperator = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(),
                    new RightRecordComparator());
            leftSortOperator.sort();
            rightSortOperator.sort();

            sortComparator = new RecordComparator();

            this.rightIterator = getRecordIterator(this.getRightTableName());
            this.leftIterator = getRecordIterator(this.getLeftTableName());

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
            marked = false;

            this.nextRecord = null;

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }
        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(proj3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }

            return nextRecord;
        }
        private void fetchNextRecord() {
            this.nextRecord = null;
            while (!hasNext()) {
                while (this.leftIterator.hasNext() && this.rightIterator.hasNext()) {
                    while (sortComparator.compare(this.leftRecord, this.rightRecord) < 0 ) {
                        leftRecord = leftIterator.next();

                    }
                    while (sortComparator.compare(this.leftRecord, this.rightRecord) > 0) {
                        rightRecord = rightIterator.next();
                    }
                    this.rightIterator.markPrev();//? next?
                    marked = true;
                    while (sortComparator.compare(this.leftRecord, this.rightRecord) == 0) {
                        while (sortComparator.compare(this.leftRecord, this.rightRecord) == 0) {
                            nextRecord = joinRecords(this.leftRecord, this.rightRecord);
                            rightRecord = rightIterator.next();
                            return;

                        }
                        if (marked == true) {
                            rightIterator.reset();
                            marked = false;
                        }
                        leftRecord = leftIterator.next();
                    }
                }
            }
        }
        private class RecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
