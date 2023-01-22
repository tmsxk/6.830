package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPage page = null;
        int pageSize = BufferPool.getPageSize();
        byte[] buf = new byte[pageSize];

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek((long) pid.getPageNumber() * pageSize);
            if (randomAccessFile.read(buf) == -1){
                return null;
            }
            page = new HeapPage((HeapPageId) pid, buf);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        PageId pageId = page.getId();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek((long) pageId.getPageNumber() * pageSize);
            randomAccessFile.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        if (!file.canRead() || !file.canWrite()) {
            throw new IOException("the needed file can't be read/written");
        }
        List<Page> res = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() == 0) {
                continue;
            }
            page.insertTuple(t);
            res.add(page);
            return res;
        }
        HeapPageId pageId = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pageId, HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> res = new ArrayList<>();
        HeapPageId pageId = (HeapPageId) t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        if (page == null) {
            throw new DbException("page is null");
        } else {
            page.deleteTuple(t);
            res.add(page);
            return res;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, Permissions.READ_ONLY);
    }

    public class HeapFileIterator implements DbFileIterator{
        TransactionId tid;
        Permissions permissions;
        Iterator<Tuple> iterator;
        int num;


        public HeapFileIterator(TransactionId tid, Permissions permissions){
            this.tid = tid;
            this.permissions = permissions;
            iterator = null;
            num = 0;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            num = 0;
            nextPage();
        }

        private void nextPage() throws TransactionAbortedException, DbException {
            HeapPageId heapPageId = new HeapPageId(getId(), num);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, permissions);
            if (page == null){
                throw new DbException("page is null");
            } else {
                iterator = page.iterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null){
                return false;
            } else if (iterator.hasNext()) {
                return true;
            } else {
                while(true) {
                    num++;
                    if (num >= numPages()) {
                        return false;
                    }
                    nextPage();
                    if (iterator.hasNext())
                        return true;
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
                throw new NoSuchElementException();
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

