package sqlancer.mysql;

import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;

@Parameters(separators = "=", commandDescription = "MySQL (default port: " + MySQLOptions.DEFAULT_PORT
        + ", default host: " + MySQLOptions.DEFAULT_HOST + ")")
public class MySQLOptions implements DBMSSpecificOptions<MySQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.TLP_WHERE);

    @Parameter(names = "--mysql-bombard", description = "Run a MySQL-only stress mode that generates and executes SQL directly over JDBC", arity = 1)
    private boolean mysqlBombard;

    @Parameter(names = "--mysql-bombard-workers", description = "Number of worker threads to run per database in MySQL bombard mode")
    private int mysqlBombardWorkers = 4;

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public boolean isMySQLBombard() {
        return mysqlBombard;
    }

    public int getMySQLBombardWorkers() {
        return mysqlBombardWorkers;
    }

}
