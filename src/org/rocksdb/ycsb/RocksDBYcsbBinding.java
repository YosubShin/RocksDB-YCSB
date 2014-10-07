package org.rocksdb.ycsb;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Yosub on 10/2/14.
 */
public class RocksDBYcsbBinding extends DB {
    private static final String DB_PATH = "/tmp/rocksdb-ycsb";
    private static final int BYTE_BUFFER_SIZE = 4096;

    RocksDB db;
    Options options;

    @Override
    public void init() throws DBException {
        System.out.println("Initializing RocksDB...");
        String db_path = DB_PATH;
        options = new Options();
        options.setCreateIfMissing(true)
                .createStatistics()
                .setWriteBufferSize(8 * SizeUnit.KB)
                .setMaxWriteBufferNumber(3)
                .setMaxBackgroundCompactions(10)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);
        Statistics stats = options.statisticsPtr();

        assert(options.createIfMissing() == true);
        assert(options.writeBufferSize() == 8 * SizeUnit.KB);
        assert(options.maxWriteBufferNumber() == 3);
        assert(options.maxBackgroundCompactions() == 10);
        assert(options.compressionType() == CompressionType.SNAPPY_COMPRESSION);
        assert(options.compactionStyle() == CompactionStyle.UNIVERSAL);

        assert(options.memTableFactoryName().equals("SkipListFactory"));
        options.setMemTableConfig(
                new HashSkipListMemTableConfig()
                        .setHeight(4)
                        .setBranchingFactor(4)
                        .setBucketCount(2000000));
        assert(options.memTableFactoryName().equals("HashSkipListRepFactory"));

        options.setMemTableConfig(
                new HashLinkedListMemTableConfig()
                        .setBucketCount(100000));
        assert(options.memTableFactoryName().equals("HashLinkedListRepFactory"));

        options.setMemTableConfig(
                new VectorMemTableConfig().setReservedSize(10000));
        assert(options.memTableFactoryName().equals("VectorRepFactory"));

        options.setMemTableConfig(new SkipListMemTableConfig());
        assert(options.memTableFactoryName().equals("SkipListFactory"));

//        options.setTableFormatConfig(new PlainTableConfig());
//        // Plain-Table requires mmap read
//        options.setAllowMmapReads(true);
//        assert(options.tableFactoryName().equals("PlainTable"));
//
//        options.setRateLimiterConfig(new GenericRateLimiterConfig(10000000,
//                10000, 10));
//        options.setRateLimiterConfig(new GenericRateLimiterConfig(10000000));
//
//        Filter bloomFilter = new BloomFilter(10);
//        BlockBasedTableConfig table_options = new BlockBasedTableConfig();
//        table_options.setBlockCacheSize(64 * SizeUnit.KB)
//                .setFilter(bloomFilter)
//                .setCacheNumShardBits(6)
//                .setBlockSizeDeviation(5)
//                .setBlockRestartInterval(10)
//                .setCacheIndexAndFilterBlocks(true)
//                .setHashIndexAllowCollision(false)
//                .setBlockCacheCompressedSize(64 * SizeUnit.KB)
//                .setBlockCacheCompressedNumShardBits(10);
//
//        assert(table_options.blockCacheSize() == 64 * SizeUnit.KB);
//        assert(table_options.cacheNumShardBits() == 6);
//        assert(table_options.blockSizeDeviation() == 5);
//        assert(table_options.blockRestartInterval() == 10);
//        assert(table_options.cacheIndexAndFilterBlocks() == true);
//        assert(table_options.hashIndexAllowCollision() == false);
//        assert(table_options.blockCacheCompressedSize() == 64 * SizeUnit.KB);
//        assert(table_options.blockCacheCompressedNumShardBits() == 10);
//
//        options.setTableFormatConfig(table_options);
//        assert(options.tableFactoryName().equals("BlockBasedTable"));

        try {
            db = RocksDB.open(options, db_path);
            db.put("hello".getBytes(), "world".getBytes());
            byte[] value = db.get("hello".getBytes());
            assert("world".equals(new String(value)));
            String str = db.getProperty("rocksdb.stats");
            assert(str != null && str != "");
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(db == null);
            assert(false);
        }

        System.out.println("Initializing RocksDB is over");
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        try {
            byte[] value = db.get(key.getBytes());
            HashMap<String, ByteIterator> deserialized = deserialize(value);
            result.putAll(deserialized);
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return 0;
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println("Scan called! NOP for now");
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            byte[] serialized = serialize(values);
            db.put(key.getBytes(), serialized);
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            byte[] serialized = serialize(values);
            db.put(key.getBytes(), serialized);
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        try {
            db.remove(key.getBytes());
        } catch (RocksDBException e) {
            System.out.format("[ERROR] caught the unexpceted exception -- %s\n", e);
            assert(false);
        }
        return 0;
    }

    @Override
    public void cleanup() throws DBException {
        super.cleanup();
        try {
            String str = db.getProperty("rocksdb.stats");
            System.out.println(str);
        } catch (RocksDBException e) {
            throw new DBException("Error while trying to print RocksDB statistics");
        }

        System.out.println("Cleaning up RocksDB database...");
//        db.close();
//        options.dispose();
        // Why does it cause error? : "pointer being freed was not allocated"
    }

    private byte[] serialize(HashMap<String, ByteIterator> values) {
        ByteBuffer buf = ByteBuffer.allocate(BYTE_BUFFER_SIZE);
        // Number of elements in HashMap (int)
        buf.put((byte) values.size());
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            // Key string length (int)
            buf.put((byte) entry.getKey().length());
            // Key bytes
            buf.put(entry.getKey().getBytes());
            // Value bytes length (long)
            buf.put((byte) entry.getValue().bytesLeft());
            // Value bytes
            buf.put((entry.getValue().toArray()));
        }

        byte[] result = new byte[buf.position()];
        buf.get(result, 0, buf.position());
        return result;
    }

    private HashMap<String, ByteIterator> deserialize(byte[] bytes) {
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int count = buf.getInt();
        for (int i = 0; i < count; i++) {
            int keyLength = buf.getInt();
            byte[] keyBytes = new byte[keyLength];
            buf.get(keyBytes, buf.position(), keyLength);

            int valueLength = buf.getInt();
            byte[] valueBytes = new byte[valueLength];
            buf.get(valueBytes, buf.position(), valueLength);

            result.put(new String(keyBytes), new ByteArrayByteIterator(valueBytes));
        }
        return result;
    }
}
