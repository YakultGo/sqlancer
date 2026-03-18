package sqlancer.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.gen.MySQLRandomQuerySynthesizer;
import sqlancer.mysql.gen.MySQLAlterTable;
import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.gen.MySQLDropIndex;
import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.mysql.gen.MySQLSetGenerator;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.mysql.gen.MySQLTruncateTableGenerator;
import sqlancer.mysql.gen.MySQLUpdateGenerator;
import sqlancer.mysql.gen.admin.MySQLFlush;
import sqlancer.mysql.gen.admin.MySQLReset;
import sqlancer.mysql.gen.datadef.MySQLIndexGenerator;
import sqlancer.mysql.gen.tblmaintenance.MySQLAnalyzeTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLCheckTable;
import sqlancer.mysql.gen.tblmaintenance.MySQLChecksum;
import sqlancer.mysql.gen.tblmaintenance.MySQLOptimize;
import sqlancer.mysql.gen.tblmaintenance.MySQLRepair;

@AutoService(DatabaseProvider.class)
public class MySQLProvider extends SQLProviderAdapter<MySQLGlobalState, MySQLOptions> {

    private static final int BOMBARD_MAX_TABLES = 6;

    public MySQLProvider() {
        super(MySQLGlobalState.class, MySQLOptions.class);
    }

    enum Action implements AbstractAction<MySQLGlobalState> {
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")), //
        INSERT(MySQLInsertGenerator::insertRow), //
        SET_VARIABLE(MySQLSetGenerator::set), //
        REPAIR(MySQLRepair::repair), //
        OPTIMIZE(MySQLOptimize::optimize), //
        CHECKSUM(MySQLChecksum::checksum), //
        CHECK_TABLE(MySQLCheckTable::check), //
        ANALYZE_TABLE(MySQLAnalyzeTable::analyze), //
        FLUSH(MySQLFlush::create), RESET(MySQLReset::create), CREATE_INDEX(MySQLIndexGenerator::create), //
        ALTER_TABLE(MySQLAlterTable::create), //
        TRUNCATE_TABLE(MySQLTruncateTableGenerator::generate), //
        SELECT_INFO((g) -> new SQLQueryAdapter(
                "select TABLE_NAME, ENGINE from information_schema.TABLES where table_schema = '" + g.getDatabaseName()
                        + "'")), //
        UPDATE(MySQLUpdateGenerator::create), //
        DELETE(MySQLDeleteGenerator::delete), //
        DROP_INDEX(MySQLDropIndex::generate);

        private final SQLQueryProvider<MySQLGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MySQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(MySQLGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
        case DROP_INDEX:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SHOW_TABLES:
            nrPerformed = r.getInteger(0, 1);
            break;
        case INSERT:
            nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            break;
        case REPAIR:
            nrPerformed = r.getInteger(0, 1);
            break;
        case SET_VARIABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case CREATE_INDEX:
            nrPerformed = r.getInteger(0, 5);
            break;
        case FLUSH:
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case OPTIMIZE:
            // seems to yield low CPU utilization
            nrPerformed = Randomly.getBooleanWithSmallProbability() ? r.getInteger(0, 1) : 0;
            break;
        case RESET:
            // affects the global state, so do not execute
            nrPerformed = globalState.getOptions().getNumberConcurrentThreads() == 1 ? r.getInteger(0, 1) : 0;
            break;
        case CHECKSUM:
        case CHECK_TABLE:
        case ANALYZE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case ALTER_TABLE:
            nrPerformed = r.getInteger(0, 5);
            break;
        case TRUNCATE_TABLE:
            nrPerformed = r.getInteger(0, 2);
            break;
        case SELECT_INFO:
            nrPerformed = r.getInteger(0, 10);
            break;
        case UPDATE:
            nrPerformed = r.getInteger(0, 10);
            break;
        case DELETE:
            nrPerformed = r.getInteger(0, 10);
            break;
        default:
            throw new AssertionError(a);
        }
        return nrPerformed;
    }

    public SQLQueryAdapter getQueryForBombard(MySQLGlobalState globalState, long workerId, long sequence)
            throws Exception {
        int tableCount = globalState.getSchema().getDatabaseTables().size();
        if (tableCount < BOMBARD_MAX_TABLES && shouldCreateBombardTable(tableCount)) {
            return MySQLTableGenerator.generate(globalState, getBombardTableName(workerId, sequence));
        }
        if (Randomly.getBoolean()) {
            String query = MySQLVisitor
                    .asString(MySQLRandomQuerySynthesizer.generate(globalState, Randomly.smallNumber() + 1)) + ";";
            return new SQLQueryAdapter(query);
        }
        return getWeightedBombardAction(globalState).getQuery(globalState);
    }

    private boolean shouldCreateBombardTable(int tableCount) {
        if (tableCount == 0) {
            return false;
        }
        if (tableCount < 2) {
            return Randomly.getBooleanWithSmallProbability();
        }
        return tableCount < BOMBARD_MAX_TABLES && Randomly.getBooleanWithRatherLowProbability();
    }

    private Action getWeightedBombardAction(MySQLGlobalState globalState) {
        List<Action> availableActions = new ArrayList<>();
        List<Integer> weights = new ArrayList<>();
        int totalWeight = 0;
        for (Action action : Action.values()) {
            if (action == Action.RESET) {
                continue;
            }
            int weight = mapActions(globalState, action);
            if (weight <= 0) {
                continue;
            }
            availableActions.add(action);
            weights.add(weight);
            totalWeight += weight;
        }
        if (availableActions.isEmpty()) {
            throw new AssertionError("No bombard actions available");
        }
        int selection = globalState.getRandomly().getInteger(0, totalWeight);
        int current = 0;
        for (int i = 0; i < availableActions.size(); i++) {
            current += weights.get(i);
            if (selection < current) {
                return availableActions.get(i);
            }
        }
        return availableActions.get(availableActions.size() - 1);
    }

    private static String getBombardTableName(long workerId, long sequence) {
        return String.format("tb%d_%d", workerId, sequence);
    }

    @Override
    public void generateDatabase(MySQLGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.getNotCachedInteger(1, 2)) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
            SQLQueryAdapter createTable = MySQLTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<MySQLGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                MySQLProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();

        if (globalState.getDbmsSpecificOptions().getTestOracleFactory().stream()
                .anyMatch((o) -> o == MySQLOracleFactory.CERT)) {
            // Enfore statistic collected for all tables
            ExpectedErrors errors = new ExpectedErrors();
            MySQLErrors.addExpressionErrors(errors);
            for (MySQLTable table : globalState.getSchema().getDatabaseTables()) {
                StringBuilder sb = new StringBuilder();
                sb.append("ANALYZE TABLE ");
                sb.append(table.getName());
                sb.append(" UPDATE HISTOGRAM ON ");
                String columns = table.getColumns().stream().map(MySQLColumn::getName)
                        .collect(Collectors.joining(", "));
                sb.append(columns + ";");
                globalState.executeStatement(new SQLQueryAdapter(sb.toString(), errors));
            }
        }
    }

    @Override
    public SQLConnection createDatabase(MySQLGlobalState globalState) throws SQLException {
        String databaseName = globalState.getDatabaseName();
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        globalState.getState().logStatement("CREATE DATABASE " + databaseName);
        globalState.getState().logStatement("USE " + databaseName);
        return createDatabase(globalState.getOptions(), databaseName);
    }

    public SQLConnection createDatabase(MainOptions options, String databaseName) throws SQLException {
        Connection con = DriverManager.getConnection(createBaseJdbcUrl(options)
                + "?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true", options.getUserName(),
                options.getPassword());
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + databaseName);
        }
        return new SQLConnection(con);
    }

    public SQLConnection createDatabaseConnection(MainOptions options, String databaseName) throws SQLException {
        Connection con = DriverManager.getConnection(createDatabaseJdbcUrl(options, databaseName), options.getUserName(),
                options.getPassword());
        return new SQLConnection(con);
    }

    private String createBaseJdbcUrl(MainOptions options) {
        String host = options.getHost();
        int port = options.getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        return String.format("jdbc:mysql://%s:%d", host, port);
    }

    private String createDatabaseJdbcUrl(MainOptions options, String databaseName) {
        return String.format("%s/%s?serverTimezone=UTC&useSSL=false&allowPublicKeyRetrieval=true",
                createBaseJdbcUrl(options), databaseName);
    }

    @Override
    public String getDBMSName() {
        return "mysql";
    }

    @Override
    public boolean addRowsToAllTables(MySQLGlobalState globalState) throws Exception {
        List<MySQLTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (MySQLTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = MySQLInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
