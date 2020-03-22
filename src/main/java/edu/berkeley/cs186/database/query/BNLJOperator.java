package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     *  See lecture slides.
     *
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         *
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            if (!leftIterator.hasNext()) {
                leftRecordIterator = null;
                leftRecord = null;
            } else {
                this.leftRecordIterator = getBlockIterator(this.getLeftTableName(), leftIterator, numBuffers - 2);
            //check empty
                this.leftRecord = this.leftRecordIterator.next();
                this.leftRecordIterator.markPrev();
            }
        }
        /**
         * Fetch the next nonempty page from the right relation.
         *
         * rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         *
         * If there are no more pages in the right relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            if (rightIterator.hasNext()) {
                //Page newRightPage = rightIterator.next();
                //rightRecordIterator
                //should be set to a record iterator over the next page of the right relation???
                this.rightRecordIterator = getBlockIterator(getRightTableName(), rightIterator,1);
                this.rightRecordIterator.markNext();
            } else {
                this.rightRecordIterator = null;
            }

        }
        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        //1. if currently have left record & right (record??) iterator has a next and right&left records are joinable -> join(leftrecord, rightrecord)
        //2. the right record iterator no next, left record iterator has a next
        // -> advance left record if I can if I can't check if right iterator hasNext && reset rightrecorditerator
        //3. the right record iterator & left record iterator both no next, but right iterator has a next
        // -> advance right page (fetchNextRightPage()) && reset left iterator
        //4. the right record iterator & left record iterator & right iterator no next but left iterator has a next
        // -> advance left iterator? fetchNextBlcok(), reset rightIterator = first page of the block & fetch right page()
        //5. the right record iterator & the right iterator both no next AND thr left record iterator & left iterator both no next
        // -> no more record throw nosuchexception
        // 9999: Solved the issue. On the last fetch I threw an exception after getting an equal record. So hasNext() change nextRecord to null again through the try-catch part.
        // for the other tests, "I see! My bug was caused because I made a mistake on the condition on when to fetch a new right page. Thank you for your time illustrating the problem."
        private void fetchNextRecord() {
            this.nextRecord = null;
            while (!hasNext()) {
                if (this.leftRecord != null && this.rightRecordIterator.hasNext() && this.rightRecordIterator != null) {
                    DataBox leftVal = this.leftRecord.getValues().get(getLeftColumnIndex());
                    Record rightRecord = this.rightRecordIterator.next();
                    DataBox rightVal = rightRecord.getValues().get(getRightColumnIndex());
                    if (leftVal.equals(rightVal)) {
                        this.nextRecord = joinRecords(leftRecord, rightRecord);
                    }
                } else if (this.leftRecordIterator != null && this.leftRecordIterator.hasNext() && this.rightRecordIterator != null) { //current rightRecordIterator (not next)
                    this.leftRecord = this.leftRecordIterator.next();
                    this.rightRecordIterator.reset();
                    this.rightRecordIterator.markNext();

                } else if (this.rightIterator.hasNext()) {
                    fetchNextRightPage();
                    this.leftRecordIterator.reset();
                    this.leftRecord = this.leftRecordIterator.next();
                    this.leftIterator.markPrev();

                } else if (this.leftIterator.hasNext()) {
                    fetchNextLeftBlock();
                    this.rightIterator.reset();
                    this.rightIterator.markNext();
                    this.fetchNextRightPage();

                }  else {
                    throw new NoSuchElementException("NoSuchElementException");
                }
            }

        }
        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         * @param leftRecord Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
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

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
