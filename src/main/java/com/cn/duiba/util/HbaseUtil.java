package com.cn.duiba.util;

import com.alibaba.jstorm.metric.Bytes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * xugf at 2017-10-12
 */
public class HbaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);
    private Connection connection = null;
    //volatile 禁止jvm进行指令重排
    private static Map<String, HbaseUtil> instanceMap = new HashMap<>();
    private static final int saveBatchSize = 100;
    private String environment;
    private String prefix;

    public static void main(String[] args) throws Exception {

    }

    public static HbaseUtil getInstance(String environment, String prefix) {
        if (instanceMap.get(prefix) == null) {
            init(environment, prefix);
        }
        return instanceMap.get(prefix);
    }

    private synchronized static void init(String environment, String prefix) {
        if (instanceMap.get(prefix) == null) {
            instanceMap.put(prefix, new HbaseUtil(environment, prefix));
        }
    }

    private HbaseUtil() {

    }

    private HbaseUtil(String environment, String prefix) {
        this.environment = environment;
        this.prefix = prefix;
        initConnection(environment, prefix);
    }

    private Connection getConnection() {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        return initConnection(environment, prefix);
    }

    private synchronized Connection initConnection(String environment, String prefix) {
        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        try {
            //加载配置文件
            PropertyUtil instance = PropertyUtil.getInstance(environment);
            //获得hbase connection连接池
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", instance.getProperty(prefix + ".hbase.zklist"));
            connection = ConnectionFactory.createConnection(conf);
            return connection;
        } catch (Exception e) {
            logger.error("initConnection error", e);
        }
        return null;
    }

    private Table getTable(String tableName) throws Exception {
        return getConnection().getTable(TableName.valueOf(tableName));
    }

    private Table getTable(String nameSpace, String tableName) throws Exception {
        return getConnection().getTable(TableName.valueOf(nameSpace, tableName));
    }

    private void closeTable(Table table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (Exception e) {
            logger.error("closeTable error.", e);
        }
    }

    /**
     * 往hbase 表中插入一条记录（单列）
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param column    列
     * @param value     值
     */
    public void insert(String tableName, String rowKey, String family, String column, String value) {
        Table table = null;
        try {
            table = getTable(tableName);
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
            table.put(put);
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入一条记录（单列）
     *
     * @param tableName  表名
     * @param rowKeyList 主键列表
     * @param family     列族
     * @param column     列
     * @param value      值
     */
    public void insert(String nameSpace, String tableName, Collection<String> rowKeyList, String family, String column, String value) {
        Table table = null;
        try {
            table = getTable(nameSpace, tableName);
            if (CollectionUtils.isNotEmpty(rowKeyList)) {
                List<Put> putList = new ArrayList<>();
                for (String rowKey : rowKeyList) {
                    Put put = new Put(rowKey.getBytes());
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                    putList.add(put);
                    //批量写入数据到hbase中
                    if (putList.size() >= saveBatchSize) {
                        table.put(putList);
                        putList.clear();
                    }
                }
                //将剩余数据写入hbase中
                if (putList.size() > 0) {
                    table.put(putList);
                }
            }

        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入一条记录（多列）
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param columns   列名称和值的键值对
     */
    public void insert(String tableName, String rowKey, String family, Map<String, String> columns) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (columns != null && columns.size() > 0) {
                Put put = new Put(rowKey.getBytes());
                for (String column : columns.keySet()) {
                    String value = columns.get(column);
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                }
                table.put(put);
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入一条记录（多列）
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param columns   列名称和值的键值对
     */
    public void insert(String nameSpace, String tableName, String rowKey, String family, Map<String, String> columns) {
        Table table = null;
        try {
            table = getTable(nameSpace, tableName);
            if (columns != null && columns.size() > 0) {
                Put put = new Put(rowKey.getBytes());
                for (String column : columns.keySet()) {
                    String value = columns.get(column);
                    put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                }
                table.put(put);
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入多条记录（多列）
     *
     * @param tableName 表名
     * @param rowKeyMap 键值对
     * @param family    列族
     */
    public void insert(String tableName, Map<String, Map<String, String>> rowKeyMap, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyMap != null && rowKeyMap.size() > 0) {
                List<Put> putList = new ArrayList<>();
                for (String rowKey : rowKeyMap.keySet()) {
                    Put put = new Put(rowKey.getBytes());
                    Map<String, String> columns = rowKeyMap.get(rowKey);
                    if (columns != null && columns.size() > 0) {
                        for (String column : columns.keySet()) {
                            String value = columns.get(column);
                            put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                        }
                    }
                    putList.add(put);

                    //批量写入数据到hbase中
                    if (putList.size() >= saveBatchSize) {
                        table.put(putList);
                        putList.clear();
                    }
                }

                //将剩余数据写入hbase中
                if (putList.size() > 0) {
                    table.put(putList);
                }
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入多条记录（多列）
     *
     * @param tableName 表名
     * @param rowKeyMap 键值对
     * @param family    列族
     */
    public void insert(String nameSpace, String tableName, Map<String, Map<String, String>> rowKeyMap, String family) {
        Table table = null;
        try {
            table = getTable(nameSpace, tableName);
            if (rowKeyMap != null && rowKeyMap.size() > 0) {
                List<Put> putList = new ArrayList<>();
                for (String rowKey : rowKeyMap.keySet()) {
                    Put put = new Put(rowKey.getBytes());
                    Map<String, String> columns = rowKeyMap.get(rowKey);
                    if (columns != null && columns.size() > 0) {
                        for (String column : columns.keySet()) {
                            String value = columns.get(column);
                            put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
                        }
                    }
                    putList.add(put);

                    //批量写入数据到hbase中
                    if (putList.size() >= saveBatchSize) {
                        table.put(putList);
                        putList.clear();
                    }
                }

                //将剩余数据写入hbase中
                if (putList.size() > 0) {
                    table.put(putList);
                }
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入多条记录（多列）
     *
     * @param tableName 表名
     * @param rowKeyMap 键值对
     * @param family    列族
     */
    public void insertLong(String tableName, Map<String, Map<String, Long>> rowKeyMap, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyMap != null && rowKeyMap.size() > 0) {
                List<Put> putList = new ArrayList<>();
                for (String rowKey : rowKeyMap.keySet()) {
                    Put put = new Put(rowKey.getBytes());
                    Map<String, Long> columns = rowKeyMap.get(rowKey);
                    if (columns != null && columns.size() > 0) {
                        for (String column : columns.keySet()) {
                            Long value = columns.get(column);
                            put.addColumn(family.getBytes(), column.getBytes(), Bytes.toBytes(value));
                        }
                    }
                    putList.add(put);

                    //批量写入数据到hbase中
                    if (putList.size() >= saveBatchSize) {
                        table.put(putList);
                        putList.clear();
                    }
                }

                //将剩余数据写入hbase中
                if (putList.size() > 0) {
                    table.put(putList);
                }
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase 表中插入多条记录（多列）
     *
     * @param tableName 表名
     * @param rowKeyMap 键值对
     * @param family    列族
     */
    public void batchRowInsert(String tableName, Map<String, Map<String, Long>> rowKeyMap, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyMap != null && rowKeyMap.size() > 0) {
                List<Put> putList = new ArrayList<>();
                for (String rowKey : rowKeyMap.keySet()) {
                    Put put = new Put(rowKey.getBytes());
                    Map<String, Long> columns = rowKeyMap.get(rowKey);
                    if (columns != null && columns.size() > 0) {
                        for (String column : columns.keySet()) {
                            Long value = columns.get(column);
                            put.addColumn(family.getBytes(), column.getBytes(), org.apache.hadoop.hbase.util.Bytes.toBytes(value));
                        }
                    }
                    putList.add(put);

                    //批量写入数据到hbase中
                    if (putList.size() >= saveBatchSize) {
                        table.put(putList);
                        putList.clear();
                    }
                }

                //将剩余数据写入hbase中
                if (putList.size() > 0) {
                    table.put(putList);
                }
            }
        } catch (Exception e) {
            logger.error("insert error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase表中累加数据（单列）
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param column    列
     * @param value     值
     */
    public void incrementColumnValue(String tableName, String rowKey, String family, String column, Long value) {
        Table table = null;
        try {
            table = getTable(tableName);
            table.incrementColumnValue(rowKey.getBytes(), family.getBytes(), column.getBytes(), value);
        } catch (Exception e) {
            logger.error("incrementColumnValue error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 往hbase表中累加数据（多列）
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param columns   列名称和值的键值对
     */
    public void incrementColumnValue(String tableName, String rowKey, String family, Map<String, Long> columns) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (columns != null && columns.size() > 0) {
                Increment increment = new Increment(rowKey.getBytes());
                for (String column : columns.keySet()) {
                    Long value = columns.get(column);
                    increment.addColumn(family.getBytes(), column.getBytes(), value);
                }
                table.increment(increment);
            }
        } catch (Exception e) {
            logger.error("incrementColumnValue error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 批量添加计数器(多列)
     *
     * @param tableName  表名
     * @param rowkeyList 主键
     * @param family     列族
     * @param columns    列名称和值的键值对
     */
    public void incrementColumnValueBatch(String tableName, List<String> rowkeyList, String family, Map<String, Long> columns) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowkeyList != null && rowkeyList.size() > 0) {
                List<Increment> incrementList = new ArrayList<>();
                for (String rowkey : rowkeyList) {
                    Increment increment = new Increment(rowkey.getBytes());
                    for (String column : columns.keySet()) {
                        Long value = columns.get(column);
                        increment.addColumn(family.getBytes(), column.getBytes(), value);
                    }
                    incrementList.add(increment);

                    //批量写入数据到hbase中
                    if (incrementList.size() >= saveBatchSize) {
                        Object[] results = new Object[incrementList.size()];
                        table.batch(incrementList, results);
                        incrementList.clear();
                    }
                }

                //将剩余数据写入到hbase中
                if (incrementList.size() > 0) {
                    Object[] results = new Object[incrementList.size()];
                    table.batch(incrementList, results);
                }
            }
        } catch (Exception e) {
            logger.error("incrementColumnValueBatch error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 批量添加计数器(多列)
     *
     * @param tableName 表名
     * @param family    列族
     * @param cfs       rowkey和column键值对
     */
    public void incrementColumnValueBatch(String tableName, String family, Map<String, Map<String, Long>> cfs) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (cfs != null && cfs.size() > 0) {
                List<Increment> incrementList = new ArrayList<>();
                for (String rowkey : cfs.keySet()) {
                    Increment increment = new Increment(rowkey.getBytes());
                    Map<String, Long> columns = cfs.get(rowkey);
                    for (String column : columns.keySet()) {
                        Long value = columns.get(column);
                        increment.addColumn(family.getBytes(), column.getBytes(), value);
                    }
                    incrementList.add(increment);

                    //批量写入数据到hbase中
                    if (incrementList.size() >= saveBatchSize) {
                        Object[] results = new Object[incrementList.size()];
                        table.batch(incrementList, results);
                        incrementList.clear();
                    }
                }

                //将剩余数据写入到hbase中
                if (incrementList.size() > 0) {
                    Object[] results = new Object[incrementList.size()];
                    table.batch(incrementList, results);
                }
            }
        } catch (Exception e) {
            logger.error("incrementColumnValueBatch error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 批量添加计数器(单列)
     *
     * @param tableName  表名
     * @param rowkeyList 主键
     * @param family     列族
     * @param column     列
     * @param value      值
     */
    public void incrementColumnValueBatch(String tableName, List<String> rowkeyList, String family, String column, Long value) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowkeyList != null && rowkeyList.size() > 0) {
                List<Increment> incrementList = new ArrayList<>();
                for (String rowkey : rowkeyList) {
                    Increment increment = new Increment(rowkey.getBytes());
                    increment.addColumn(family.getBytes(), column.getBytes(), value);
                    incrementList.add(increment);

                    //批量写入数据到hbase中
                    if (incrementList.size() >= saveBatchSize) {
                        Object[] results = new Object[incrementList.size()];
                        table.batch(incrementList, results);
                        incrementList.clear();
                    }
                }

                //将剩余数据写入到hbase中
                if (incrementList.size() > 0) {
                    Object[] results = new Object[incrementList.size()];
                    table.batch(incrementList, results);
                }
            }
        } catch (Exception e) {
            logger.error("incrementColumnValueBatch error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 判断rowkey是否存在
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @return rowkey是否存在，存在=true
     */
    public Boolean exists(String tableName, String rowKey) {
        Table table = null;
        try {
            table = getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            return table.exists(get);
        } catch (Exception e) {
            logger.error("getRow error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 查询列族下的某一列数据
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param column    列
     * @return 结果数据
     */
    public Result getRow(String tableName, String rowKey, String family, String column) {
        Table table = null;
        try {
            table = getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            get.addColumn(family.getBytes(), column.getBytes());
            return table.get(get);
        } catch (Exception e) {
            logger.error("getRow error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }


    public Result getRowAllColumn(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());
            return table.get(get);
        } catch (Exception e) {
            logger.error("getRow error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 查询列族下的某一列数据(多条记录)
     *
     * @param tableName  表名
     * @param rowKeyList 主键列表
     * @param family     列族
     * @param column     列
     * @return 结果数据
     */
    public Result[] getRowList(String tableName, List<String> rowKeyList, String family, String column) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyList != null && rowKeyList.size() > 0) {
                List<Get> getList = new ArrayList<>();
                for (String rowKey : rowKeyList) {
                    Get get = new Get(rowKey.getBytes());
                    get.addColumn(family.getBytes(), column.getBytes());
                    getList.add(get);
                }
                return table.get(getList);
            }
        } catch (Exception e) {
            logger.error("getRow error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 查询列族下的多列数据
     *
     * @param tableName 表名
     * @param rowKey    主键
     * @param family    列族
     * @param columns   列
     * @return 结果数据
     */
    public Result getRow(String tableName, String rowKey, String family, List<String> columns) {
        Table table = null;
        try {
            table = getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            if (columns != null && columns.size() > 0) {
                for (String column : columns) {
                    get.addColumn(family.getBytes(), column.getBytes());
                }
            }
            return table.get(get);
        } catch (Exception e) {
            logger.error("getRow error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 查询列族下的多列数据(多条记录)
     *
     * @param tableName  表名
     * @param rowKeyList 主键列表
     * @param family     列族
     * @param columns    列
     * @return 结果数据
     */
    public Result[] getRowList(String tableName, List<String> rowKeyList, String family, List<String> columns) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyList != null && rowKeyList.size() > 0) {
                List<Get> getList = new ArrayList<>();
                for (String rowKey : rowKeyList) {
                    Get get = new Get(rowKey.getBytes());
                    if (columns != null && columns.size() > 0) {
                        for (String column : columns) {
                            get.addColumn(family.getBytes(), column.getBytes());
                        }
                    }
                    getList.add(get);
                }
                return table.get(getList);
            }
        } catch (Exception e) {
            logger.error("getRowList error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 查询列族下的多列数据(多条记录)
     *
     * @param tableName  表名
     * @param rowKeyList 主键列表
     * @param family     列族
     * @return 结果数据
     */
    public Result[] getRowList(String tableName, List<String> rowKeyList, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyList != null && rowKeyList.size() > 0) {
                List<Get> getList = new ArrayList<>();
                for (String rowKey : rowKeyList) {
                    Get get = new Get(rowKey.getBytes());
                    get.addFamily(family.getBytes());
                    getList.add(get);
                }
                return table.get(getList);
            }
        } catch (Exception e) {
            logger.error("getRowList error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }


    /**
     * 查询列族下的多列数据(多条记录)
     *
     * @param tableName 表名
     * @param rowKeyMap 键值对
     * @param family    列族
     * @return 结果数据
     */
    public Result[] getRowList(String tableName, Map<String, List<String>> rowKeyMap, List<String> rowkeyList, String family) {
        Table table = null;
        try {
            table = getTable(tableName);
            if (rowKeyMap != null && rowKeyMap.size() > 0) {
                List<Get> getList = new ArrayList<>();
                for (String rowKey : rowkeyList) {
                    List<String> columns = rowKeyMap.get(rowKey);
                    Get get = new Get(rowKey.getBytes());
                    for (String column : columns) {
                        get.addColumn(family.getBytes(), column.getBytes());
                    }
                    getList.add(get);
                }
                return table.get(getList);
            }
        } catch (Exception e) {
            logger.error("getRowList error", e);
        } finally {
            closeTable(table);
        }
        return null;
    }

    /**
     * 批量删除表数据
     *
     * @param tableName  表名称
     * @param rowKeyList rowkey列表
     */
    public void batchDelete(String tableName, List<String> rowKeyList) {
        Table table = null;
        try {
            if (null != rowKeyList && rowKeyList.size() > 0) {
                table = getTable(tableName);
                List<Delete> deleteList = new ArrayList<>();
                for (String rowKey : rowKeyList) {
                    Delete delete = new Delete(rowKey.getBytes());
                    deleteList.add(delete);
                }
                table.delete(deleteList);
            }
        } catch (Exception e) {
            logger.error("batchDelete error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 删除表数据
     *
     * @param tableName 表名称
     * @param rowKey    rowkey
     */
    public void delete(String tableName, String rowKey) {
        Table table = null;
        try {
            if (StringUtils.isNotBlank(rowKey)) {
                table = getTable(tableName);
                Delete delete = new Delete(rowKey.getBytes());
                table.delete(delete);
            }
        } catch (Exception e) {
            logger.error("batchDelete error", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 删除表数据
     *
     * @param tableName 表名称
     * @param rowKey    rowkey
     */
    public void delete(String nameSpace, String tableName, String rowKey) {
        Table table = null;
        try {
            if (StringUtils.isNotBlank(rowKey)) {
                table = getTable(nameSpace, tableName);
                Delete delete = new Delete(rowKey.getBytes());
                table.delete(delete);
            }
        } catch (Exception e) {
            logger.error("batchDelete error", e);
        } finally {
            closeTable(table);
        }
    }

}
