package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//import com.sun.xml.internal.ws.client.sei.ResponseBuilder;
import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.relations.ColumnType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;


/**
 * This class implements the TupleFile interface for heap files.
 */
public class HeapTupleFile implements TupleFile {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapTupleFile.class);


    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;

    /**
     * The manager for heap tuple files provides some higher-level operations
     * such as saving the metadata of a heap tuple file, so it's useful to
     * have a reference to it.
     */
    private HeapTupleFileManager heapFileManager;

    /** The schema of tuples in this tuple file. */
    private TableSchema schema;


    /** Statistics for this tuple file. */
    private TableStats stats;


    /** The file that stores the tuples. */
    private DBFile dbFile;


    public HeapTupleFile(StorageManager storageManager,
                         HeapTupleFileManager heapFileManager, DBFile dbFile,
                         TableSchema schema, TableStats stats) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (heapFileManager == null)
            throw new IllegalArgumentException("heapFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        this.storageManager = storageManager;
        this.heapFileManager = heapFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
    }


    @Override
    public TupleFileManager getManager() {
        return heapFileManager;
    }


    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple() throws IOException {
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.

            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page...

                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    return new HeapFilePageTuple(schema, dbPage, iSlot, offset);
                }

                // If we got here, the page has no tuples.  Unpin the page.
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                         ".  Returning null.");
        }

        return null;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                " on page " + fptr.getPageNo() + " is empty.");
        }

        return new HeapFilePageTuple(schema, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple,
     * or <tt>null</tt> if there are no more tuples in the file.
     **/
    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        DBFile dbFile = dbPage.getDBFile();

        int nextSlot = ptup.getSlot() + 1;
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    return new HeapFilePageTuple(schema, dbPage, nextSlot,
                                                 nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                DBPage nextDBPage =
                    storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                dbPage.unpin();
                dbPage = nextDBPage;

                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                dbPage.unpin();
                return null;
            }
        }

        // It's pretty gross to have no return statement here, but there's
        // no way to reach this point.
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        /*
         * Check to see whether any constraints are violated by
         * adding this tuple
         *
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        // Get Size of the tuple you're adding
        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                    " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Search for a page to put the tuple in.  If we hit the end of the
        // data file, create a new page.
        DBPage freeDBPage = null;
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0, false);

        int freePageNo =  (int) HeaderPage.getNextFreeDataPageNo(headerPage);
        int prevPageNo = 0;

        while (freePageNo != -1) {

            freeDBPage = storageManager.loadDBPage(dbFile, freePageNo, false);
            int freeSpace = DataPage.getFreeSpaceInPage(freeDBPage) - 4;
            if (freeSpace >= tupSize + 2) {
                break;
            }

            prevPageNo = freePageNo;
            freePageNo = DataPage.getNextFreeDataPageNo(freeDBPage);
            freeDBPage.unpin();
        }

        // Add page if no new free pages
        if (freePageNo == -1) {

            // Adding the new page
            freePageNo = dbFile.getNumPages();
            logger.debug("Creating new page " + freePageNo +
                    " to store new tuple.");
            freeDBPage = storageManager.loadDBPage(dbFile, freePageNo, true);
            DataPage.initNewPage(freeDBPage);

            // Linking up the new page to the tail and making the new page
            // the tail
            int tailPageNo = HeaderPage.getTailFreeDataPageNo(headerPage);
            DBPage tailPage = storageManager.loadDBPage(dbFile, tailPageNo);

            if (HeaderPage.getNextFreeDataPageNo(headerPage) == -1) {
                HeaderPage.setNextFreeDataPageNo(headerPage, freePageNo);
            }
            else {
                DataPage.setNextFreeDataPageNo(tailPage, freePageNo);
            }

            HeaderPage.setTailFreeDataPageNo(headerPage, freePageNo);

            storageManager.logDBPageWrite(tailPage);
            tailPage.unpin();
        }

        // Write the tuple data into the page
        int slot = DataPage.allocNewTuple(freeDBPage, tupSize);
        int tupOffset = DataPage.getSlotValue(freeDBPage, slot);

        logger.debug(String.format(
                "New tuple will reside on page %d, slot %d.", freePageNo, slot));

        HeapFilePageTuple pageTup = HeapFilePageTuple.storeNewTuple(schema,
                freeDBPage, slot, tupOffset, tup);

        // If the DataPage is now full, then we remove it from the list
        int freeSpace = DataPage.getFreeSpaceInPage(freeDBPage) - 4;

        if(freeSpace < minTupleSize(tup))
        {
            DBPage prevDBPage = storageManager.loadDBPage(dbFile,
                    prevPageNo, false);

            // If tail is the node that is full, get rid of tail
            // set the second to last node to point to one and to tail
            if (freePageNo == HeaderPage.getTailFreeDataPageNo(headerPage)) {
                DataPage.setNextFreeDataPageNo(prevDBPage, -1);
                HeaderPage.setTailFreeDataPageNo(headerPage, prevPageNo);
            }

            // Otherwise
            else {
                int nextPageNo = DataPage.getNextFreeDataPageNo(freeDBPage);
                if (prevPageNo == 0) {
                    HeaderPage.setNextFreeDataPageNo(prevDBPage, nextPageNo);
                }
                else {
                    DataPage.setNextFreeDataPageNo(prevDBPage, nextPageNo);
                }
            }

            // Log changes to the WAL
            storageManager.logDBPageWrite(prevDBPage);
            prevDBPage.unpin();
        }

        DataPage.sanityCheck(freeDBPage);

        // Log all the changes to the write ahead log
        storageManager.logDBPageWrite(freeDBPage);
        storageManager.logDBPageWrite(headerPage);

        // Unpin page after adding tuple
        freeDBPage.unpin();
        headerPage.unpin();

        return pageTup;
    }



    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issues.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DBPage dbPage = ptup.getDBPage();
        DataPage.sanityCheck(dbPage);
        storageManager.logDBPageWrite(dbPage);
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(Tuple tup) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();

        int freeSpace = DataPage.getFreeSpaceInPage(dbPage) - 4;

        boolean fullFlag = false;

        if (minTupleSize(ptup) > freeSpace) {
            fullFlag = true;
        }


        DataPage.deleteTuple(dbPage, ptup.getSlot());

        // Freestyle
        if (fullFlag) {

            DBPage headerPage = storageManager.loadDBPage(dbFile, 0, false);
            int fullPageNo = dbPage.getPageNo();

            // Set new datapage prev to tail
            int tailPageNo = HeaderPage.getTailFreeDataPageNo(headerPage);
            DBPage tailPage = storageManager.loadDBPage(dbFile, tailPageNo);
            DataPage.setNextFreeDataPageNo(tailPage, fullPageNo);

            // Set tail to new page
            HeaderPage.setTailFreeDataPageNo(headerPage, fullPageNo);

            // Have to set the new page's next value as the end
            DataPage.setNextFreeDataPageNo(dbPage, -1);

            // Performed an update to the next pointers, so we need to log
            // the changes
            storageManager.logDBPageWrite(headerPage);
            storageManager.logDBPageWrite(tailPage);

            headerPage.unpin();
            tailPage.unpin();
        }

        // Deleted tuple from dbPage, so we need to log to the WAL
        storageManager.logDBPageWrite(dbPage);

        DataPage.sanityCheck(dbPage);
    }

    public int minTupleSize(Tuple tup) {
        int numColumns = tup.getColumnCount();
        int sum = 0;
        for(int i = 0; i < numColumns; i++) {
            ColumnType columnType = schema.getColumnInfo(i).getType();
            sum += PageTuple.getStorageSize(columnType, 0);
        }
        return sum;
    }

    @Override
    public void analyze() throws IOException {
        // Keep track of number of Tuples
        int numTuples = 0;

        // Keep track of total Tuple Size in bytes to get the average tuple
        // size later
        float totalTupleSize = 0;

        // Number of Pages is total number of pages minus the header page
        int numPages = dbFile.getNumPages() - 1;

        // Initialize a ColumnStatsCollector for each column
        ArrayList<ColumnStatsCollector> columns = new ArrayList<ColumnStatsCollector>();
        for (int i = 0; i < schema.numColumns(); i++) {
            columns.add(new ColumnStatsCollector(schema.getColumnInfo(i).getType().getBaseType()));
        }

        // Scan through the data pages until we hit the end of the table
        // file.  It may be that the first run of data pages is empty,
        // so just keep looking until we hit the end of the file.

        // Header page is page 0, so first data page is page 1.

        for (int iPage = 1; iPage <= numPages ; iPage++) {
            DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
            totalTupleSize += DataPage.getTupleDataEnd(dbPage)
                    - DataPage.getTupleDataStart(dbPage);

            int numSlots = DataPage.getNumSlots(dbPage);
            for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                // Get the offset of the tuple in the page.  If it's 0 then
                // the slot is empty, and we skip to the next slot.
                int offset = DataPage.getSlotValue(dbPage, iSlot);
                if (offset == DataPage.EMPTY_SLOT)
                    continue;

                // Get the next tuple
                HeapFilePageTuple currTuple = new
                        HeapFilePageTuple(schema, dbPage, iSlot, offset);

                // Update tuple stats
                numTuples += 1;

                // Update column stats using ColumnStatsCollector
                for (int i = 0; i < currTuple.getColumnCount(); i++) {
                    columns.get(i).addValue(currTuple.getColumnValue(i));
                }

            }

            // We are done with the page.  Unpin the page.
            dbPage.unpin();
        }


        // Save the column stats for each column and store in stats
        ArrayList<ColumnStats> columnStats = new ArrayList<ColumnStats>();
        for (ColumnStatsCollector column: columns) {
            columnStats.add(column.getColumnStats());
        }
        stats = new TableStats(numPages,
                numTuples, totalTupleSize / numTuples, columnStats);

        // Save the statistics
        heapFileManager.saveMetadata(this);
    }

    @Override
    public List<String> verify() throws IOException {
        // TODO!
        // Right now we will just report that everything is fine.
        return new ArrayList<String>();
    }


    @Override
    public void optimize() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
