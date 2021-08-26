package de.crafttogether;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

public class MySQLAdapterPlugin extends JavaPlugin {

    @Override
    public void onEnable() {
        Bukkit.getServer().getLogger().info("MySQLAdapter v" + this.getDescription().getVersion() + " loaded");
    }

    @Override
    public void onDisable() {
        Bukkit.getServer().getLogger().info("MySQLAdapter v" + this.getDescription().getVersion() + " unloaded");
    }
}
