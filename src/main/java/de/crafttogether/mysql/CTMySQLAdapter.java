package de.crafttogether.mysql;

import com.zaxxer.hikari.pool.HikariPool;
import de.crafttogether.exception.AdapterInitializationException;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.ArrayList;
import java.util.Collection;

public class CTMySQLAdapter extends JavaPlugin {
    private static CTMySQLAdapter plugin;
    private static MySQLPool pool;

    static final Collection<MySQLPool> openPools = new ArrayList<>();

    @Override
    public void onEnable() {
        plugin = this;

        // Load configuration
        saveDefaultConfig();
        FileConfiguration config = getConfig();

        if (config.getBoolean("Settings.Connect")) {
            // Setup MySQLConfig
            MySQLConfig myCfg = new MySQLConfig();
            myCfg.setHost(config.getString("MySQL.Host"));
            myCfg.setPort(config.getInt("MySQL.Port"));
            myCfg.setUsername(config.getString("MySQL.Username"));
            myCfg.setPassword(config.getString("MySQL.Password"));

            // Validate configuration
            if (!myCfg.checkInputs()) {
                getLogger().warning("[MySQL]: Invalid configuration! Please check your config.yml");
                getServer().getPluginManager().disablePlugin(this);
                return;
            }

            // Initialize MySQLAdapter
            pool = new MySQLPool(this, myCfg);
        }

        getLogger().info("MySQLAdapter v" + this.getDescription().getVersion() + " loaded");
    }

    @Override
    public void onDisable() {
        // Close connection-pool
        if (pool != null)
            pool.close();

        getLogger().info("MySQLAdapter v" + this.getDescription().getVersion() + " unloaded");
    }

    public static MySQLPool getPool() throws AdapterInitializationException { return pool; }

    public static MySQLConnection getConnection() {
        try { return getPool().getConnection(); }
        catch (AdapterInitializationException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    static Collection<MySQLPool> getOpenPools() { return openPools; }

    static Collection<MySQLConnection> getAllOpenConnections() {
        Collection<MySQLConnection> pools = new ArrayList<>();
        for (MySQLPool pool : openPools) pools.addAll(pool.getOpenConnections());
        return pools;
    }

    public static CTMySQLAdapter instance() { return plugin; }
}
