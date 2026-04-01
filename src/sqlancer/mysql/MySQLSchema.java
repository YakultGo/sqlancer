package sqlancer.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractRowValue;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTable.MySQLEngine;
import sqlancer.mysql.ast.MySQLConstant;

public class MySQLSchema extends AbstractSchema<MySQLGlobalState, MySQLTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum MySQLDataType {
        INT, VARCHAR, FLOAT, DOUBLE, DECIMAL;

        public static MySQLDataType getRandom(MySQLGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(MySQLDataType.INT, MySQLDataType.VARCHAR);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DOUBLE:
            case FLOAT:
            case DECIMAL:
                return true;
            case VARCHAR:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

    }

    public static class MySQLColumn extends AbstractTableColumn<MySQLTable, MySQLDataType> {

        private final boolean isPrimaryKey;
        private final int precision;

        public enum CollateSequence {
            NOCASE, RTRIM, BINARY;

            public static CollateSequence random() {
                return Randomly.fromOptions(values());
            }
        }

        public MySQLColumn(String name, MySQLDataType columnType, boolean isPrimaryKey, int precision) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.precision = precision;
        }

        public int getPrecision() {
            return precision;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

    }

    public static class MySQLTables extends AbstractTables<MySQLTable, MySQLColumn> {

        public MySQLTables(List<MySQLTable> tables) {
            super(tables);
        }

        public MySQLRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            String randomRow = String.format("SELECT %s FROM %s ORDER BY RAND() LIMIT 1", columnNamesAsString(
                    c -> c.getTable().getName() + "." + c.getName() + " AS " + c.getTable().getName() + c.getName()),
                    // columnNamesAsString(c -> "typeof(" + c.getTable().getName() + "." +
                    // c.getName() + ")")
                    tableNamesAsString());
            Map<MySQLColumn, MySQLConstant> values = new HashMap<>();
            try (Statement s = con.createStatement()) {
                ResultSet randomRowValues = s.executeQuery(randomRow);
                if (!randomRowValues.next()) {
                    throw new AssertionError("could not find random row! " + randomRow + "\n");
                }
                for (int i = 0; i < getColumns().size(); i++) {
                    MySQLColumn column = getColumns().get(i);
                    Object value;
                    int columnIndex = randomRowValues.findColumn(column.getTable().getName() + column.getName());
                    assert columnIndex == i + 1;
                    MySQLConstant constant;
                    if (randomRowValues.getString(columnIndex) == null) {
                        constant = MySQLConstant.createNullConstant();
                    } else {
                        switch (column.getType()) {
                        case INT:
                            value = randomRowValues.getLong(columnIndex);
                            constant = MySQLConstant.createIntConstant((long) value);
                            break;
                        case VARCHAR:
                            value = randomRowValues.getString(columnIndex);
                            constant = MySQLConstant.createStringConstant((String) value);
                            break;
                        default:
                            throw new AssertionError(column.getType());
                        }
                    }
                    values.put(column, constant);
                }
                assert !randomRowValues.next();
                return new MySQLRowValue(this, values);
            }

        }

    }

    private static MySQLDataType getColumnType(String typeString) {
        switch (typeString) {
        case "tinyint":
        case "smallint":
        case "mediumint":
        case "int":
        case "bigint":
            return MySQLDataType.INT;
        case "varchar":
        case "tinytext":
        case "mediumtext":
        case "text":
        case "longtext":
            return MySQLDataType.VARCHAR;
        case "double":
            return MySQLDataType.DOUBLE;
        case "float":
            return MySQLDataType.FLOAT;
        case "decimal":
            return MySQLDataType.DECIMAL;
        default:
            throw new AssertionError(typeString);
        }
    }

    public static class MySQLRowValue extends AbstractRowValue<MySQLTables, MySQLColumn, MySQLConstant> {

        MySQLRowValue(MySQLTables tables, Map<MySQLColumn, MySQLConstant> values) {
            super(tables, values);
        }

    }

    public static class MySQLTable extends AbstractRelationalTable<MySQLColumn, MySQLIndex, MySQLGlobalState> {

        public enum MySQLEngine {
            INNO_DB("InnoDB"), MY_ISAM("MyISAM"), MEMORY("MEMORY"), HEAP("HEAP"), CSV("CSV"), MERGE("MERGE"),
            ARCHIVE("ARCHIVE"), FEDERATED("FEDERATED");

            private String s;

            MySQLEngine(String s) {
                this.s = s;
            }

            public static MySQLEngine get(String val) {
                return Stream.of(values()).filter(engine -> engine.s.equalsIgnoreCase(val)).findFirst().get();
            }

        }

        private final MySQLEngine engine;

        public MySQLTable(String tableName, List<MySQLColumn> columns, List<MySQLIndex> indexes, MySQLEngine engine) {
            super(tableName, columns, indexes, false /* TODO: support views */);
            this.engine = engine;
        }

        public MySQLEngine getEngine() {
            return engine;
        }

        public boolean hasPrimaryKey() {
            return getColumns().stream().anyMatch(c -> c.isPrimaryKey());
        }

    }

    public static final class MySQLIndex extends TableIndex {

        private MySQLIndex(String indexName) {
            super(indexName);
        }

        public static MySQLIndex create(String indexName) {
            return new MySQLIndex(indexName);
        }

        @Override
        public String getIndexName() {
            if (super.getIndexName().contentEquals("PRIMARY")) {
                return "`PRIMARY`";
            } else {
                return super.getIndexName();
            }
        }

    }

    public enum MySQLTriggerTiming {
        BEFORE, AFTER;

        public static MySQLTriggerTiming get(String value) {
            return Stream.of(values()).filter(v -> v.name().equalsIgnoreCase(value)).findFirst()
                    .orElseThrow(() -> new AssertionError(value));
        }
    }

    public enum MySQLTriggerEvent {
        INSERT, UPDATE, DELETE;

        public static MySQLTriggerEvent get(String value) {
            return Stream.of(values()).filter(v -> v.name().equalsIgnoreCase(value)).findFirst()
                    .orElseThrow(() -> new AssertionError(value));
        }
    }

    public static final class MySQLTrigger {

        private final String name;
        private final MySQLTable baseTable;
        private final MySQLTriggerTiming timing;
        private final MySQLTriggerEvent event;

        public MySQLTrigger(String name, MySQLTable baseTable, MySQLTriggerTiming timing, MySQLTriggerEvent event) {
            this.name = name;
            this.baseTable = baseTable;
            this.timing = timing;
            this.event = event;
        }

        public String getName() {
            return name;
        }

        public MySQLTable getBaseTable() {
            return baseTable;
        }

        public MySQLTriggerTiming getTiming() {
            return timing;
        }

        public MySQLTriggerEvent getEvent() {
            return event;
        }
    }

    private final List<MySQLTrigger> triggers;

    public static MySQLSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        /* the loop is a workaround for https://bugs.mysql.com/bug.php?id=95929 */
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<MySQLTable> databaseTables = new ArrayList<>();
                Map<String, MySQLTable> tablesByName = new HashMap<>();
                try (Statement s = con.createStatement()) {
                    try (ResultSet rs = s.executeQuery(
                            "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '"
                                    + databaseName + "';")) {
                        while (rs.next()) {
                            String tableName = rs.getString("TABLE_NAME");
                            String tableEngineStr = rs.getString("ENGINE");
                            MySQLEngine engine = MySQLEngine.get(tableEngineStr);
                            List<MySQLColumn> databaseColumns = getTableColumns(con, tableName, databaseName);
                            List<MySQLIndex> indexes = getIndexes(con, tableName, databaseName);
                            MySQLTable t = new MySQLTable(tableName, databaseColumns, indexes, engine);
                            for (MySQLColumn c : databaseColumns) {
                                c.setTable(t);
                            }
                            databaseTables.add(t);
                            tablesByName.put(tableName, t);
                        }
                    }
                }
                List<MySQLTrigger> databaseTriggers = getTriggers(con, databaseName, tablesByName);
                return new MySQLSchema(databaseTables, databaseTriggers);
            } catch (SQLIntegrityConstraintViolationException e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    private static List<MySQLTrigger> getTriggers(SQLConnection con, String databaseName,
            Map<String, MySQLTable> tablesByName) throws SQLException {
        List<MySQLTrigger> triggers = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT TRIGGER_NAME, EVENT_OBJECT_TABLE, ACTION_TIMING, EVENT_MANIPULATION FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = '%s';",
                    databaseName))) {
                while (rs.next()) {
                    String tableName = rs.getString("EVENT_OBJECT_TABLE");
                    MySQLTable baseTable = tablesByName.get(tableName);
                    if (baseTable == null) {
                        continue;
                    }
                    triggers.add(new MySQLTrigger(rs.getString("TRIGGER_NAME"), baseTable,
                            MySQLTriggerTiming.get(rs.getString("ACTION_TIMING")),
                            MySQLTriggerEvent.get(rs.getString("EVENT_MANIPULATION"))));
                }
            }
        }
        return triggers;
    }

    private static List<MySQLIndex> getIndexes(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLIndex> indexes = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format(
                    "SELECT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s';",
                    databaseName, tableName))) {
                while (rs.next()) {
                    String indexName = rs.getString("INDEX_NAME");
                    indexes.add(MySQLIndex.create(indexName));
                }
            }
        }
        return indexes;
    }

    private static List<MySQLColumn> getTableColumns(SQLConnection con, String tableName, String databaseName)
            throws SQLException {
        List<MySQLColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("select * from information_schema.columns where table_schema = '"
                    + databaseName + "' AND TABLE_NAME='" + tableName + "'")) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("DATA_TYPE");
                    int precision = rs.getInt("NUMERIC_PRECISION");
                    boolean isPrimaryKey = rs.getString("COLUMN_KEY").equals("PRI");
                    MySQLColumn c = new MySQLColumn(columnName, getColumnType(dataType), isPrimaryKey, precision);
                    columns.add(c);
                }
            }
        }
        return columns;
    }

    public MySQLSchema(List<MySQLTable> databaseTables, List<MySQLTrigger> triggers) {
        super(databaseTables);
        this.triggers = List.copyOf(triggers);
    }

    public MySQLSchema(List<MySQLTable> databaseTables) {
        this(databaseTables, List.of());
    }

    public MySQLTables getRandomTableNonEmptyTables() {
        return new MySQLTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    public List<MySQLTrigger> getTriggers() {
        return triggers;
    }

    public boolean hasTrigger(MySQLTable table, MySQLTriggerEvent event) {
        return triggers.stream().anyMatch(t -> t.getBaseTable() == table && t.getEvent() == event);
    }

    public boolean hasTrigger(MySQLTable table, MySQLTriggerTiming timing, MySQLTriggerEvent event) {
        return triggers.stream()
                .anyMatch(t -> t.getBaseTable() == table && t.getTiming() == timing && t.getEvent() == event);
    }

    public boolean hasAnyTrigger(MySQLTable table) {
        return triggers.stream().anyMatch(t -> t.getBaseTable() == table);
    }

    public String getFreeTriggerName() {
        int i = 0;
        if (Randomly.getBooleanWithRatherLowProbability()) {
            i = (int) Randomly.getNotCachedInteger(0, 100);
        }
        do {
            String triggerName = String.format("tr%d", i++);
            if (triggers.stream().noneMatch(t -> t.getName().equalsIgnoreCase(triggerName))) {
                return triggerName;
            }
        } while (true);
    }

    public MySQLTable getRandomTableWeightedByTrigger(MySQLTriggerEvent event) {
        return getRandomTableWeightedByTrigger(event, t -> !t.isView());
    }

    public MySQLTable getRandomTableWeightedByTrigger(MySQLTriggerEvent event, Predicate<MySQLTable> predicate) {
        List<MySQLTable> tables = getDatabaseTables().stream().filter(predicate).collect(Collectors.toList());
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        int totalWeight = 0;
        List<Integer> weights = new ArrayList<>();
        for (MySQLTable table : tables) {
            int weight = hasTrigger(table, event) ? 5 : 1;
            totalWeight += weight;
            weights.add(weight);
        }
        int selection = (int) Randomly.getNotCachedInteger(0, totalWeight);
        int current = 0;
        for (int i = 0; i < tables.size(); i++) {
            current += weights.get(i);
            if (selection < current) {
                return tables.get(i);
            }
        }
        return tables.get(tables.size() - 1);
    }

    public MySQLTable getSafeOtherTableForTrigger(MySQLTable baseTable) {
        List<MySQLTable> withoutTriggers = getDatabaseTables().stream()
                .filter(t -> t != baseTable && !t.isView() && !hasAnyTrigger(t)).collect(Collectors.toList());
        if (!withoutTriggers.isEmpty()) {
            return Randomly.fromList(withoutTriggers);
        }
        throw new IgnoreMeException();
    }

}
