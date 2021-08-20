package de.crafttogether.example;

import de.crafttogether.MySQLAdapter;
import de.crafttogether.MySQLConnection;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLExample {

    private void insertSync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        String name = "Bob";
        int age = 18;

        try {
            connection.insert("INSERT INTO `%spersons` " +
            "(" +
                "`name`, " +
                "`age`" +
            ") " +

            "VALUES (" +
                "'%s', " +
                "%d" +
            ");", connection.getTablePrefix(), name, age);
        }

        catch (SQLException ex) {
            ex.printStackTrace();
        }

        finally {
            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }
    }

    private void insertAsync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        String name = "Bob";
        int age = 18;

        connection.insertAsync("INSERT INTO `%spersons` " +
        "(" +
            "`name`, " +
            "`age`" +
        ") " +

        "VALUES (" +
            "'%s', " +
            "%d" +
        ");",

        // Process Result
        (err, lastInsertedId) -> {
            if (err != null)
                err.printStackTrace();

            Integer personId = lastInsertedId;

            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }, connection.getTablePrefix(), name, age);
    }

    private void querySync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        try {
            ResultSet result = connection.query("SELECT * FROM `%spersons`", connection.getTablePrefix());

            // Process Results
            while (result.next()) {
                int id = result.getInt("id");
                String name = result.getString("name");
                int age = result.getInt("age");

                System.out.println("User #" + id + " (" + name + ") is " + age + " years old.");
            }
        }

        catch (SQLException ex) {
            ex.printStackTrace();
        }

        finally {
            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }
    }

    private void queryAsync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        connection.queryAsync("SELECT * FROM `%spersons`", (err, result) -> {
            if (err != null)
                err.printStackTrace();

            // Process Results
            try {
                while (result.next()) {
                    String name = result.getString("name");
                    Integer age = result.getInt("age");
                    System.out.println(name + " is " + age + " years old.");
                }
            }

            catch (SQLException ex) {
                ex.printStackTrace();
            }

            finally {
                connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
            }
        }, connection.getTablePrefix());
    }

    private void updateSync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        int userId = 1;
        String name = "Bob";
        int age = 12;

        int affectedRows = 0;

        try {
            affectedRows = connection.update("UPDATE `%spersons` SET " +
                "`name` = '%s', " +
                "`age`  = %d, " +
            "WHERE `id` = %d;", connection.getTablePrefix(), name, age, userId);
        }

        catch (SQLException ex) {
            ex.printStackTrace();
        }

        finally {
            System.out.println(affectedRows  + " where updated.");
            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }
    }

    private void updateAsync()    {
        MySQLConnection connection = MySQLAdapter.getConnection();

        int userId = 1;
        String name = "Bob";
        int age = 12;

        connection.updateAsync("UPDATE `%spersons` SET " +
            "`name` = '%s', " +
            "`age`  = %d, " +
        "WHERE `id` = %d;",

        (err, affectedRows) -> {
            if (err != null)
                err.printStackTrace();

            System.out.println(affectedRows  + " where updated.");
            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }, connection.getTablePrefix(), name, age, userId);
    }

    private void executeSync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        String statement =
        """
            CREATE TABLE `%spersons` (
              `id` int(11) NOT NULL,
              `name` varchar(255) NOT NULL,
              `age` int(4) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
                        
            ALTER TABLE `%spersons`
              ADD PRIMARY KEY (`id`);
                        
            ALTER TABLE `%spersons`
              MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
            COMMIT;
        """;

        boolean success = false;
        try {
            success = connection.execute(statement, connection.getTablePrefix());
        }

        catch (SQLException ex) {
            ex.printStackTrace();
        }

        finally {
            if (success)
                System.out.println("Statement executed successfully .");

            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }
    }

    private void executeAsync() {
        MySQLConnection connection = MySQLAdapter.getConnection();

        String statement =
        """
            CREATE TABLE `%spersons` (
              `id` int(11) NOT NULL,
              `name` varchar(255) NOT NULL,
              `age` int(4) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
                        
            ALTER TABLE `%spersons`
              ADD PRIMARY KEY (`id`);
                        
            ALTER TABLE `%spersons`
              MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;
            COMMIT;
        """;

        connection.executeAsync(statement, (err, success) -> {
            if (err != null)
                err.printStackTrace();

            if (success)
                System.out.println("Statement executed successfully .");

            connection.close(); // Close preparedStatement (if set), resultSet (if set) & connection
        }, connection.getTablePrefix());
    }
}
