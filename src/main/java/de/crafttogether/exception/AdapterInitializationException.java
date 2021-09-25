package de.crafttogether.exception;

public class AdapterInitializationException extends Throwable {

    public AdapterInitializationException() {
        super("CTMySQLAdapter is not connected. Please check your config.yml!");
    }
}
