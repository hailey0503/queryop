package edu.berkeley.cs186.database.index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Supplier;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.BufferManagerImpl;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import edu.berkeley.cs186.database.categories.*;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.table.RecordId;

import static org.junit.Assert.*;

@Category(Proj2Tests.class)
public class TestBPlusTree {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // max 5 I/Os per iterator creation default
    private static final int MAX_IO_PER_ITER_CREATE = 5;

    // max 1 I/Os per iterator next, unless overridden
    private static final int MAX_IO_PER_NEXT = 1;

    // 3 seconds max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
                3000 * TimeoutScaling.factor)));

    @Before
    public void setup()  {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManagerImpl(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        this.metadata = null;
    }

    @After
    public void cleanup() {
        this.bufferManager.close();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                                              0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    private BPlusTree getBPlusTree(Type keySchema, int order) {
        setBPlusTreeMetadata(keySchema, order);
        return new BPlusTree(bufferManager, metadata, treeContext);
    }

    // the 0th item in maxIOsOverride specifies how many I/Os constructing the iterator may take
    // the i+1th item in maxIOsOverride specifies how many I/Os the ith call to next() may take
    // if there are more items in the iterator than maxIOsOverride, then we default to
    // MAX_IO_PER_ITER_CREATE/MAX_IO_PER_NEXT once we run out of items in maxIOsOverride
    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier,
                                            Iterator<Integer> maxIOsOverride) {
        bufferManager.evictAll();

        long initialIOs = bufferManager.getNumIOs();

        long prevIOs = initialIOs;
        Iterator<T> iter = iteratorSupplier.get();
        long newIOs = bufferManager.getNumIOs();
        long maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_ITER_CREATE;
        assertFalse("too many I/Os used constructing iterator (" + (newIOs - prevIOs) + " > " + maxIOs +
                    ") - are you materializing more than you need?",
                    newIOs - prevIOs > maxIOs);

        List<T> xs = new ArrayList<>();
        while (iter.hasNext()) {
            prevIOs = bufferManager.getNumIOs();
            xs.add(iter.next());
            newIOs = bufferManager.getNumIOs();
            maxIOs = maxIOsOverride.hasNext() ? maxIOsOverride.next() : MAX_IO_PER_NEXT;
            assertFalse("too many I/Os used per next() call (" + (newIOs - prevIOs) + " > " + maxIOs +
                        ") - are you materializing more than you need?",
                        newIOs - prevIOs > maxIOs);
        }

        long finalIOs = bufferManager.getNumIOs();
        maxIOs = xs.size() / (2 * metadata.getOrder());
        assertTrue("too few I/Os used overall (" + (finalIOs - initialIOs) + " < " + maxIOs +
                   ") - are you materializing before the iterator is even constructed?",
                   (finalIOs - initialIOs) >= maxIOs);
        return xs;
    }

    private <T> List<T> indexIteratorToList(Supplier<Iterator<T>> iteratorSupplier) {
        return indexIteratorToList(iteratorSupplier, Collections.emptyIterator());
    }

    // Tests ///////////////////////////////////////////////////////////////////

    @Test
    @Category(PublicTests.class)
    public void testSimpleBulkLoad() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        assertEquals("()", tree.toSexp());

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 1; i <= 11; ++i) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
        }

        tree.bulkLoad(data.iterator(), fillFactor);
        //      (    4        7         10        _   )
        //       /       |         |         \
        // (1 2 3 _) (4 5 6 _) (7 8 9 _) (10 11 _ _)
        String leaf0 = "((1 (1 1)) (2 (2 2)) (3 (3 3)))";
        String leaf1 = "((4 (4 4)) (5 (5 5)) (6 (6 6)))";
        String leaf2 = "((7 (7 7)) (8 (8 8)) (9 (9 9)))";
        String leaf3 = "((10 (10 10)) (11 (11 11)))";
        String sexp = String.format("(%s 4 %s 7 %s 10 %s)", leaf0, leaf1, leaf2, leaf3);
        assertEquals(sexp, tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testWhiteBoxTest() {
        BPlusTree tree = getBPlusTree(Type.intType(), 1);
        assertEquals("()", tree.toSexp());

        // (4)
        tree.put(new IntDataBox(4), new RecordId(4, (short) 4));
        assertEquals("((4 (4 4)))", tree.toSexp());

        // (4 9)
        tree.put(new IntDataBox(9), new RecordId(9, (short) 9));
        assertEquals("((4 (4 4)) (9 (9 9)))", tree.toSexp());

        //   (6)
        //  /   \
        // (4) (6 9)
        tree.put(new IntDataBox(6), new RecordId(6, (short) 6));
        String l = "((4 (4 4)))";
        String r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //     (6)
        //    /   \
        // (2 4) (6 9)
        tree.put(new IntDataBox(2), new RecordId(2, (short) 2));
        l = "((2 (2 2)) (4 (4 4)))";
        r = "((6 (6 6)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s)", l, r), tree.toSexp());

        //      (6 7)
        //     /  |  \
        // (2 4) (6) (7 9)
        tree.put(new IntDataBox(7), new RecordId(7, (short) 7));
        l = "((2 (2 2)) (4 (4 4)))";
        String m = "((6 (6 6)))";
        r = "((7 (7 7)) (9 (9 9)))";
        assertEquals(String.format("(%s 6 %s 7 %s)", l, m, r), tree.toSexp());

        //         (7)
        //        /   \
        //     (6)     (8)
        //    /   \   /   \
        // (2 4) (6) (7) (8 9)
        tree.put(new IntDataBox(8), new RecordId(8, (short) 8));
        String ll = "((2 (2 2)) (4 (4 4)))";
        String lr = "((6 (6 6)))";
        String rl = "((7 (7 7)))";
        String rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 6 %s)", ll, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (7)
        //           /   \
        //     (3 6)       (8)
        //   /   |   \    /   \
        // (2) (3 4) (6) (7) (8 9)
        tree.put(new IntDataBox(3), new RecordId(3, (short) 3));
        ll = "((2 (2 2)))";
        String lm = "((3 (3 3)) (4 (4 4)))";
        lr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s 6 %s)", ll, lm, lr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 7 %s)", l, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //   (3)      (6)       (8)
        //  /   \    /   \    /   \
        // (2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(5), new RecordId(5, (short) 5));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        String ml = "((4 (4 4)) (5 (5 5)))";
        String mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (1 2) (3) (4 5) (6) (7) (8 9)
        tree.put(new IntDataBox(1), new RecordId(1, (short) 1));
        ll = "((1 (1 1)) (2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8 9)
        tree.remove(new IntDataBox(1));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)) (9 (9 9)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) (6) (7) (8  )
        tree.remove(new IntDataBox(9));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "((6 (6 6)))";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (4 5) ( ) (7) (8  )
        tree.remove(new IntDataBox(6));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((4 (4 4)) (5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (  2) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(4));
        ll = "((2 (2 2)))";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (  5) ( ) (7) (8  )
        tree.remove(new IntDataBox(2));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "((5 (5 5)))";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) (7) (8  )
        tree.remove(new IntDataBox(5));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "((7 (7 7)))";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) (3) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(7));
        ll = "()";
        lr = "((3 (3 3)))";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (8  )
        tree.remove(new IntDataBox(3));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "((8 (8 8)))";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());

        //            (4 7)
        //           /  |  \
        //    (3)      (6)       (8)
        //   /   \    /   \    /   \
        // (   ) ( ) (   ) ( ) ( ) (   )
        tree.remove(new IntDataBox(8));
        ll = "()";
        lr = "()";
        ml = "()";
        mr = "()";
        rl = "()";
        rr = "()";
        l = String.format("(%s 3 %s)", ll, lr);
        m = String.format("(%s 6 %s)", ml, mr);
        r = String.format("(%s 8 %s)", rl, rr);
        assertEquals(String.format("(%s 4 %s 7 %s)", l, m, r), tree.toSexp());
    }

    @Test
    @Category(PublicTests.class)
    public void testRandomPuts() {
        List<DataBox> keys = new ArrayList<>();
        List<RecordId> rids = new ArrayList<>();
        List<RecordId> sortedRids = new ArrayList<>();
        for (int i = 0; i < 1000; ++i) {
            keys.add(new IntDataBox(i));
            rids.add(new RecordId(i, (short) i));
            sortedRids.add(new RecordId(i, (short) i));
        }

        // Try trees with different orders.
        for (int d = 2; d < 5; ++d) {
            // Try trees with different insertion orders.
            for (int n = 0; n < 2; ++n) {
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));

                // Insert all the keys.
                BPlusTree tree = getBPlusTree(Type.intType(), d);
                for (int i = 0; i < keys.size(); ++i) {
                    tree.put(keys.get(i), rids.get(i));
                }

                // Test get.
                for (int i = 0; i < keys.size(); ++i) {
                    assertEquals(Optional.of(rids.get(i)), tree.get(keys.get(i)));
                }

                // Test scanAll.
                assertEquals(sortedRids, indexIteratorToList(tree::scanAll));

                // Test scanGreaterEqual.
                for (int i = 0; i < keys.size(); i += 100) {
                    final int j = i;
                    List<RecordId> expected = sortedRids.subList(i, sortedRids.size());
                    assertEquals(expected, indexIteratorToList(() -> tree.scanGreaterEqual(new IntDataBox(j))));
                }

                // Load the tree from disk.
                BPlusTree fromDisk = new BPlusTree(bufferManager, metadata, treeContext);
                assertEquals(sortedRids, indexIteratorToList(fromDisk::scanAll));

                // Test remove.
                Collections.shuffle(keys, new Random(42));
                Collections.shuffle(rids, new Random(42));
                for (DataBox key : keys) {
                    fromDisk.remove(key);
                    assertEquals(Optional.empty(), fromDisk.get(key));
                }
            }
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testMaxOrder() {
        // Note that this white box test depend critically on the implementation
        // of toBytes and includes a lot of magic numbers that won't make sense
        // unless you read toBytes.
        assertEquals(4, Type.intType().getSizeInBytes());
        assertEquals(8, Type.longType().getSizeInBytes());
        assertEquals(10, RecordId.getSizeInBytes());
        short pageSizeInBytes = 100;
        Type keySchema = Type.intType();
        assertEquals(3, LeafNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, InnerNode.maxOrder(pageSizeInBytes, keySchema));
        assertEquals(3, BPlusTree.maxOrder(pageSizeInBytes, keySchema));
    }

    @Test
    public void testEverythingDeleted() {
        BPlusTree tree = getBPlusTree(Type.intType(), 2);
        float fillFactor = 0.75f;
        assertEquals("()", tree.toSexp());
        System.out.println(tree.toSexp());
        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 1; i <= 11; ++i) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(i, (short) i)));
        }

        tree.bulkLoad(data.iterator(), fillFactor);
        assertEquals(true, tree.scanAll().hasNext());
        System.out.println(tree.toSexp());
        Iterator<RecordId> iter = tree.scanAll();
        for (int i = 1; i <= 11; ++i) {
            tree.remove(new IntDataBox(i));
        }
        assertEquals(false, tree.scanAll().hasNext());
        System.out.println(tree.toSexp());
    }
}
