package de.crafttogether.mysql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.plugin.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public class MySQLPool {
    final Plugin plugin;
    final CTMySQLAdapter corePlugin;
    final MySQLConfig config;
    final HikariConfig hikariConfig;
    final HikariDataSource dataSource;
    final Collection<MySQLConnection> connections;

    public MySQLPool(Plugin bukkitPlugin, MySQLConfig config) {
        this.plugin = bukkitPlugin;
        this.corePlugin = CTMySQLAdapter.instance();
        this.config = config;
        this.hikariConfig = configure();
        this.dataSource = new HikariDataSource(this.hikariConfig);
        this.connections = new ArrayList<>();

        if (corePlugin.getConfig().getBoolean("Settings.Debug"))
            plugin.getLogger().info("(DEBUG) Created new connection-pool for '" + this.plugin.getDescription().getName() + "'");

        CTMySQLAdapter.openPools.add(this);
    }

    public MySQLPool(Plugin bukkitPlugin, String host, int port, String database, String username, String password, String tablePrefix) {
        this.plugin = bukkitPlugin;
        this.corePlugin = CTMySQLAdapter.instance();
        this.config = new MySQLConfig(host, port, database, username, password, tablePrefix);
        this.hikariConfig = configure();
        this.dataSource = new HikariDataSource(this.hikariConfig);
        this.connections = new ArrayList<>();

        if (corePlugin.getConfig().getBoolean("Settings.Debug"))
            plugin.getLogger().info("(DEBUG) Created new connection-pool for '" + this.plugin.getDescription().getName() + "'");

        CTMySQLAdapter.openPools.add(this);
    }

    private HikariConfig configure() {
        HikariConfig config = new HikariConfig();

        config.setDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
        config.addDataSourceProperty("serverName", this.config.getHost());
        config.addDataSourceProperty("port", this.config.getPort());
        if (this.config.getDatabase() != null) config.addDataSourceProperty("databaseName", this.config.getDatabase());

        config.setUsername(this.config.getUsername());
        config.setPassword(this.config.getPassword());

        config.setMaximumPoolSize(10);
        config.setMinimumIdle(1);
        config.setIdleTimeout(10000);
        config.setMaxLifetime(60000);
        config.setAutoCommit(true);

        return config;
    }

    public MySQLConnection getConnection() {
        MySQLConnection connection = new MySQLConnection(this);
        this.connections.add(connection);
        return connection;
    }

    public MySQLConfig getConfig() {
        return this.config;
    }
    public Collection<MySQLConnection> getOpenConnections() { return this.connections; }

    public HikariDataSource getDataSource() { return dataSource; }
    public HikariConfig getHikariConfig() { return hikariConfig; }

    public void close() {
        for (MySQLConnection connection : connections) connection.close();
        this.dataSource.close();

        if (corePlugin.getConfig().getBoolean("Settings.Debug"))
            plugin.getLogger().info("(DEBUG) Closed connection-pool of '" + this.plugin.getDescription().getName() + "'");

        CTMySQLAdapter.openPools.remove(this);
    }
}