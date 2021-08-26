package de.crafttogether.mysql;

import com.zaxxer.hikari.HikariDataSource;
import de.crafttogether.Callback;
import org.bukkit.Bukkit;
import org.bukkit.plugin.Plugin;
import org.jetbrains.annotations.Nullable;

import java.sql.*;

public class MySQLConnection {
    private Plugin plugin;

    private HikariDataSource dataSource;
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;

    public  MySQLConnection(HikariDataSource dataSource, Plugin bukkitPlugin) {
        this.dataSource = dataSource;
        this.plugin = bukkitPlugin;
    }

    private void executeAsync(Runnable task) {
        Bukkit.getServer().getScheduler().runTaskAsynchronously(plugin, task);
    }

    public int insert(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        int lastInsertedId = 0;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement, Statement.RETURN_GENERATED_KEYS);
            preparedStatement.executeUpdate();

            resultSet = preparedStatement.getGeneratedKeys();
            if (resultSet.next())
                lastInsertedId = resultSet.getInt(1);
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return lastInsertedId;
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
                    plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, 0);
                }
            }
        });

        return this;
    }

    public ResultSet query(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            resultSet = preparedStatement.executeQuery();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return resultSet;
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
                    plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, null);
                }
            }
        });

        return this;
    }

    public int update(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        int rows = 0;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            rows = preparedStatement.executeUpdate();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return rows;
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
                    plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
                else {
                    assert callback != null;
                    callback.call(e, 0);
                }
            }
        });

        return this;
    }

    public Boolean execute(String statement, final Object... args) throws SQLException {
        if (args.length > 0) statement = String.format(statement, args);
        String finalStatement = statement;

        boolean result = false;
        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(finalStatement);
            result = preparedStatement.execute();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("link failure"))
                plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
            else
                throw e;
        }

        return result;
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
                    plugin.getLogger().warning("[MySQL]: Couldn't connect to MySQL-Server...");
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
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.out.println(e.getMessage());
            }
        }

        return this;
    }

    public String getTablePrefix() {
        return MySQLAdapter.getAdapter().getConfig().getTablePrefix();
    }
}