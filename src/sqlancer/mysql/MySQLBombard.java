package sqlancer.mysql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import sqlancer.IgnoreMeException;
import sqlancer.Main;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.gen.MySQLTableGenerator;

public final class MySQLBombard {

    private static final int SCHEMA_REFRESH_INTERVAL = 100;

    private final MySQLProvider provider;
    private final MainOptions options;
    private final MySQLOptions mysqlOptions;
    private final String databaseName;
    private final long seed;

    public MySQLBombard(MySQLProvider provider, MainOptions options, MySQLOptions mysqlOptions, String databaseName,
            long seed) {
        this.provider = provider;
        this.options = options;
        this.mysqlOptions = mysqlOptions;
        this.databaseName = databaseName;
        this.seed = seed;
    }

    public void run() throws Exception {
        try (SQLConnection con = provider.createDatabase(options, databaseName)) {
            Main.nrDatabases.incrementAndGet();
            bootstrapDatabase(con);
        }

        int workerCount = Math.max(1, mysqlOptions.getMySQLBombardWorkers());
        ExecutorService workerPool = Executors.newFixedThreadPool(workerCount);
        List<Future<?>> futures = new ArrayList<>();
        for (int workerId = 0; workerId < workerCount; workerId++) {
            final int localWorkerId = workerId;
            futures.add(workerPool.submit(() -> runWorker(localWorkerId)));
        }
        try {
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            workerPool.shutdownNow();
        }
    }

    private void runWorker(int workerId) {
        MySQLGlobalState state = createWorkerState(workerId);
        long sequence = 0;
        long refreshCounter = 0;
        try {
            if (!connect(state)) {
                return;
            }
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    SQLQueryAdapter query = provider.getQueryForBombard(state, workerId, sequence++);
                    boolean success = query.execute(state, false);
                    Main.nrQueries.incrementAndGet();
                    if (query.couldAffectSchema() || refreshCounter++ % SCHEMA_REFRESH_INTERVAL == 0) {
                        refreshSchema(state);
                    }
                    if (!success && state.getSchema().getDatabaseTables().isEmpty()) {
                        refreshSchema(state);
                    }
                } catch (IgnoreMeException ignored) {
                    refreshSchemaQuietly(state);
                } catch (SQLException e) {
                    Main.nrUnsuccessfulActions.incrementAndGet();
                    if (isConnectionException(e)) {
                        return;
                    } else {
                        refreshSchemaQuietly(state);
                    }
                } catch (AssertionError ignored) {
                    refreshSchemaQuietly(state);
                } catch (Throwable ignored) {
                    refreshSchemaQuietly(state);
                }
            }
        } finally {
            closeQuietly(state);
        }
    }

    private MySQLGlobalState createWorkerState(int workerId) {
        MySQLGlobalState state = new MySQLGlobalState();
        state.setDatabaseName(databaseName);
        state.setMainOptions(options);
        state.setDbmsSpecificOptions(mysqlOptions);
        state.setRandomly(new Randomly(seed + workerId + 1L));
        state.setState(provider.getStateToReproduce(databaseName));
        return state;
    }

    private void bootstrapDatabase(SQLConnection con) {
        MySQLGlobalState state = createWorkerState(-1);
        state.setConnection(con);
        try {
            refreshSchema(state);
            int targetTables = state.getRandomly().getInteger(1, 3);
            for (int i = state.getSchema().getDatabaseTables().size(); i < targetTables; i++) {
                SQLQueryAdapter createTable = MySQLTableGenerator.generate(state, String.format("tb_bootstrap_%d", i));
                createTable.execute(state, false);
                refreshSchemaQuietly(state);
            }
        } catch (Exception ignored) {
            refreshSchemaQuietly(state);
        } finally {
            state.setConnection(null);
        }
    }

    private boolean connect(MySQLGlobalState state) {
        closeQuietly(state);
        try {
            state.setConnection(provider.createDatabaseConnection(options, databaseName));
            refreshSchema(state);
            return true;
        } catch (SQLException e) {
            closeQuietly(state);
            return false;
        } catch (Throwable ignored) {
            closeQuietly(state);
            return false;
        }
    }

    private void refreshSchema(MySQLGlobalState state) throws Exception {
        state.updateSchema();
    }

    private void refreshSchemaQuietly(MySQLGlobalState state) {
        try {
            refreshSchema(state);
        } catch (Exception ignored) {
            // A stale local schema is acceptable in bombard mode.
        }
    }

    private void closeQuietly(MySQLGlobalState state) {
        if (state.getConnection() == null) {
            return;
        }
        try {
            state.getConnection().close();
        } catch (SQLException ignored) {
            // ignore
        } finally {
            state.setConnection(null);
        }
    }

    private boolean isConnectionException(SQLException e) {
        String sqlState = e.getSQLState();
        if (sqlState != null && sqlState.startsWith("08")) {
            return true;
        }
        Throwable current = e;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase();
                if (lower.contains("communications link failure") || lower.contains("connection reset")
                        || lower.contains("connection refused") || lower.contains("no operations allowed after connection closed")
                        || lower.contains("the last packet successfully received")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

}
