package sqlancer.mysql.gen;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTriggerEvent;
import sqlancer.mysql.MySQLSchema.MySQLTriggerTiming;

public final class MySQLTriggerGenerator {

    private final MySQLGlobalState globalState;
    private final MySQLSchema schema;

    private MySQLTriggerGenerator(MySQLGlobalState globalState) {
        this.globalState = globalState;
        this.schema = globalState.getSchema();
    }

    public static SQLQueryAdapter create(MySQLGlobalState globalState) {
        return new MySQLTriggerGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        List<TriggerSpec> specs = getPossibleSpecs();
        if (specs.isEmpty()) {
            throw new IgnoreMeException();
        }
        TriggerSpec spec = Randomly.fromList(specs);
        String triggerName = schema.getFreeTriggerName();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TRIGGER ");
        sb.append(triggerName);
        sb.append(" ");
        sb.append(spec.timing);
        sb.append(" ");
        sb.append(spec.event);
        sb.append(" ON ");
        sb.append(spec.baseTable.getName());
        sb.append(" FOR EACH ROW ");
        sb.append(spec.statement);
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addTriggerErrors(errors);
        MySQLErrors.addInsertUpdateErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

    private List<TriggerSpec> getPossibleSpecs() {
        List<TriggerSpec> specs = new ArrayList<>();
        List<MySQLTable> candidateTables = schema.getDatabaseTables().stream().filter(t -> !t.isView())
                .collect(Collectors.toList());
        if (candidateTables.isEmpty()) {
            throw new IgnoreMeException();
        }
        for (MySQLTable baseTable : candidateTables) {
            for (MySQLTriggerTiming timing : MySQLTriggerTiming.values()) {
                for (MySQLTriggerEvent event : MySQLTriggerEvent.values()) {
                    if (schema.hasTrigger(baseTable, timing, event)) {
                        continue;
                    }
                    addStatementSpecs(specs, baseTable, timing, event);
                }
            }
        }
        return specs;
    }

    private void addStatementSpecs(List<TriggerSpec> specs, MySQLTable baseTable, MySQLTriggerTiming timing,
            MySQLTriggerEvent event) {
        addSetNewSpec(specs, baseTable, timing, event);
        addSideEffectSpecs(specs, baseTable, timing, event);
    }

    private void addSetNewSpec(List<TriggerSpec> specs, MySQLTable baseTable, MySQLTriggerTiming timing,
            MySQLTriggerEvent event) {
        if (timing == MySQLTriggerTiming.BEFORE
                && (event == MySQLTriggerEvent.INSERT || event == MySQLTriggerEvent.UPDATE)) {
            List<MySQLColumn> writableColumns = getWritableColumns(baseTable);
            if (writableColumns.isEmpty()) {
                return;
            }
            MySQLColumn column = Randomly.fromList(writableColumns);
            specs.add(new TriggerSpec(baseTable, timing, event,
                    String.format("SET NEW.%s = %s", column.getName(), generateSimpleExpression(column, false))));
        }
    }

    private void addSideEffectSpecs(List<TriggerSpec> specs, MySQLTable baseTable, MySQLTriggerTiming timing,
            MySQLTriggerEvent event) {
        MySQLTable otherTable;
        try {
            otherTable = schema.getSafeOtherTableForTrigger(baseTable);
        } catch (IgnoreMeException e) {
            return;
        }

        specs.add(new TriggerSpec(baseTable, timing, event, generateInsertInto(otherTable)));
        specs.add(new TriggerSpec(baseTable, timing, event, generateUpdateOtherTable(otherTable)));
        specs.add(new TriggerSpec(baseTable, timing, event, generateDeleteFrom(otherTable)));
    }

    private String generateInsertInto(MySQLTable table) {
        List<MySQLColumn> columns = getWritableColumns(table);
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }
        return String.format("INSERT INTO %s(%s) VALUES(%s)", table.getName(),
                columns.stream().map(MySQLColumn::getName).collect(Collectors.joining(", ")),
                columns.stream().map(c -> generateSimpleExpression(c, true)).collect(Collectors.joining(", ")));
    }

    private String generateUpdateOtherTable(MySQLTable table) {
        List<MySQLColumn> writableColumns = getWritableColumns(table);
        if (writableColumns.isEmpty()) {
            throw new IgnoreMeException();
        }
        MySQLColumn updateColumn = Randomly.fromList(writableColumns);
        return String.format("UPDATE %s SET %s = %s WHERE %s", table.getName(), updateColumn.getName(),
                generateSimpleExpression(updateColumn, false), generateSimplePredicate(table));
    }

    private String generateDeleteFrom(MySQLTable table) {
        return String.format("DELETE FROM %s WHERE %s", table.getName(), generateSimplePredicate(table));
    }

    private String generateSimplePredicate(MySQLTable table) {
        MySQLColumn predicateColumn = table.getRandomColumn();
        if (Randomly.getBoolean()) {
            return predicateColumn.getName() + " IS NULL";
        }
        return predicateColumn.getName() + " = " + generateSimpleExpression(predicateColumn, false);
    }

    private List<MySQLColumn> getWritableColumns(MySQLTable table) {
        return table.getColumns().stream().filter(c -> !c.getName().startsWith("v")).collect(Collectors.toList());
    }

    private String generateSimpleExpression(MySQLColumn column, boolean allowNull) {
        if (allowNull && Randomly.getBooleanWithSmallProbability()) {
            return "NULL";
        }
        switch (column.getType()) {
        case INT:
            return String.valueOf(globalState.getRandomly().getInteger());
        case VARCHAR:
            return "'" + globalState.getRandomly().getString().replace("\\", "").replace("'", "").replace("\n", "")
                    + "'";
        case FLOAT:
        case DOUBLE:
        case DECIMAL:
            return String.valueOf(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError(column.getType());
        }
    }

    private static final class TriggerSpec {

        private final MySQLTable baseTable;
        private final MySQLTriggerTiming timing;
        private final MySQLTriggerEvent event;
        private final String statement;

        private TriggerSpec(MySQLTable baseTable, MySQLTriggerTiming timing, MySQLTriggerEvent event,
                String statement) {
            this.baseTable = baseTable;
            this.timing = timing;
            this.event = event;
            this.statement = statement;
        }
    }

}
