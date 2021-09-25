package de.crafttogether.mysql;

import de.crafttogether.Callback;
import org.apache.commons.lang.RandomStringUtils;
import org.bukkit.Bukkit;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.time.Instant;

public class MySQLConnection {
    final String connectionId;
    final MySQLPool pool;

    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    MySQLConnection(MySQLPool pool) {
        this.pool = pool;
        this.connectionId = getConnectionId();

        if (pool.corePlugin.getConfig().getBoolean("Settings.Debug")) {
            pool.corePlugin.getLogger().info("(DEBUG) Created new Connection (#" + connectionId + ") for '" + pool.plugin.getDescription().getName() + "'");
            pool.corePlugin.getLogger().info("(DEBUG) There are " + this.pool.getOpenConnections().size() + " open connections at this time");
        }
    }

    private String getConnectionId() {
        String unixTimestamp = String.valueOf(Instant.now().getEpochSecond());
        return unixTimestamp.substring(unixTimestamp.length() - 5);
    }

    private void executeAsync(Runnable task) {
        Bukkit.getServer().getScheduler().runTaskAsynchronously(pool.plugin, task);
    }

    public int insert(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        int lastInsertedId = 0;
        try {
            connection = pool.dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.executeUpdate();

            resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next())
                lastInsertedId = resultSet.getInt(1);
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return lastInsertedId;
    }

    public ResultSet query(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        try {
            connection = pool.dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            resultSet = preparedStatement.executeQuery();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return resultSet;
    }


    public int update(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        int rows = 0;
        try {
            connection = pool.dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            rows = preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return rows;
    }

    public Boolean execute(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        boolean result = false;
        try {
            connection = pool.dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            result = preparedStatement.execute();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return result;
    }

    public MySQLConnection insertAsync(String statement, final @Nullable Callback<SQLException, Integer> callback, final Object... args) {
        if (args.length > 0) statement = String.format(statement, args);
        final String finalStatement = statement;

        executeAsync(() -> {
            try {
                int lastInsertedId = insert(finalStatement);
                assert callback != null;
                callback.call(null, lastInsertedId);
            } catch (SQLException e) {
                if (e.getMessage().contains("link failure"))
                    pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, 0);
                }
            }
        });

        return this;
    }

    public MySQLConnection queryAsync(String statement, final @Nullable Callback<SQLException, ResultSet> callback, final Object... args) {
        if (args.length > 0) statement = String.format(statement, args);
        final String finalStatement = statement;

        executeAsync(() -> {
            try {
                ResultSet resultSet = query(finalStatement);
                assert callback != null;
                callback.call(null, resultSet);
            } catch (SQLException e) {
                if (e.getMessage().contains("link failure"))
                    pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, null);
                }
            }
        });

        return this;
    }

    public MySQLConnection updateAsync(String statement, final @Nullable Callback<SQLException, Integer> callback, final Object... args) {
        if (args.length > 0) statement = String.format(statement, args);
        final String finalStatement = statement;

        executeAsync(() -> {
            try {
                int rows = update(finalStatement);
                assert callback != null;
                callback.call(null, rows);
            } catch (SQLException e) {
                if (e.getMessage().contains("link failure"))
                    pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, 0);
                }
            }
        });

        return this;
    }

    public MySQLConnection executeAsync(String statement, final @Nullable Callback<SQLException, Boolean> callback, final Object... args) {
        if (args.length > 0) statement = String.format(statement, args);
        final String finalStatement = statement;

        executeAsync(() -> {
            try {
                boolean result = execute(finalStatement);
                assert callback != null;
                callback.call(null, result);
            } catch (SQLException e) {
                if (e.getMessage().contains("link failure"))
                    pool.plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, false);
                }
            }
        });

        return this;
    }

    public MySQLConnection close() {
        if (resultSet != null) {
            try {
                resultSet.close();

                if (pool.corePlugin.getConfig().getBoolean("Settings.Debug"))
                    pool.corePlugin.getLogger().info("(DEBUG) Closed ResultSet for Connection #" + connectionId + " of '" + pool.plugin.getDescription().getName() + "'");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        if (preparedStatement != null) {
            try {
                preparedStatement.close();

                if (pool.corePlugin.getConfig().getBoolean("Settings.Debug"))
                    pool.corePlugin.getLogger().info("(DEBUG) Closed PreparedStatement for Connection #" + connectionId + " of '" + pool.plugin.getDescription().getName() + "'");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        if (connection != null) {
            try {
                connection.close();

                if (pool.corePlugin.getConfig().getBoolean("Settings.Debug"))
                    pool.corePlugin.getLogger().info("(DEBUG) Closed Connection #" + connectionId + " of '" + pool.plugin.getDescription().getName() + "'");
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        pool.connections.remove(this);
        return this;
    }

    public String getTablePrefix() { return pool.getConfig().getTablePrefix(); }
    public MySQLPool getPool() { return pool; }
}