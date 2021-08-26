package de.crafttogether.mysql;

import com.zaxxer.hikari.HikariDataSource;
import org.bukkit.plugin.Plugin;

public class MySQLAdapter {
    private static MySQLAdapter adapter;

    private final Plugin plugin;
    private final MySQLConfig config;

    private HikariDataSource dataSource;

    public MySQLAdapter(Plugin pluginInstance, MySQLConfig _config) {
        plugin = pluginInstance;
        adapter = this;
        config = _config;
        setupHikari();
    }

    public MySQLAdapter(Plugin bukkitPlugin, String host, int port, String database, String username, String password, String tablePrefix) {
        adapter = this;
        this.plugin = bukkitPlugin;
        this.config = new MySQLConfig(host, port, database, username, password, tablePrefix);
        setupHikari();
    }

    private void setupHikari() {
        this.dataSource = new HikariDataSource();
        this.dataSource.setDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
        this.dataSource.addDataSourceProperty("serverName", config.getHost());
        this.dataSource.addDataSourceProperty("port", config.getPort());

        if (config.getDatabase() != null)
            this.dataSource.addDataSourceProperty("databaseName", config.getDatabase());

        this.dataSource.addDataSourceProperty("user", config.getUsername());
        this.dataSource.addDataSourceProperty("password", config.getPassword());
        this.dataSource.setAutoCommit(true);
    }

    public static MySQLAdapter getAdapter() {
        return adapter;
    }

    public static MySQLConnection getConnection() {
        return new MySQLConnection(adapter.dataSource, adapter.plugin);
    }

    public MySQLConfig getConfig() {
        return this.config;
    }

    public void disconnect() {
        // TODO: Should we call .close() on all instantiated MySQLConnection-Objects here?
        dataSource.close();
    }
}