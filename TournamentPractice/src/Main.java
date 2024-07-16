import java.sql.*;
public class Main {
    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "dndPractice";

    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";

    public static void main(String[] args) {
        try (Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {
            checkDriver();
            checkDB();
            System.out.println("Подключение к базе данных | " + DATABASE_URL + "\n");
            getTables(connection);
            getPlayers(connection);
            getCharacters(connection);
            getMasters(connection);
            getMoscowPlayers(connection);
            getSessionsInfo(connection, 4);
            ChangeStatus(connection, "Не в игре", 2);
            getProMasters(connection);
            updateHealth(connection, 2, 13);
            createNewTable(connection, 11,0, 6, 4, 5, "Не в игре");
            deleteCharacter(connection, 13);
            getCharactersClassed(connection);
            getCharactersPlayers(connection, "Женщина");
            updateDifficulty(connection, 7, 3);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Нет JDBC-драйвера! Подключите JDBC-драйвер к проекту согласно инструкции.");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try {
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.out.println("Нет базы данных! Проверьте имя базы, путь к базе или разверните локально резервную копию согласно инструкции");
            throw new RuntimeException(e);
        }
    }

    static void getTables(Connection connection) throws SQLException {
        int param0 = -1, param1 = -1, param2 = -1, param3 = -1, param4 = -1;
        String param5 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM tables ORDER BY id;");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getInt(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getString(6);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5);
        }
    }

    public static void getPlayers(Connection connection) throws SQLException {
        int param0 = -1, param2 = -1, param3 = -1, param4 = -1, param7 = -1;
        String param1 = null;
        String param5 = null;
        String param6 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM players");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getString(6);
            param6 = rs.getString(7);
            param7 = rs.getInt(8);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7);
        }
        System.out.println();
    }

    public static void getMasters(Connection connection) throws SQLException {
        int param0 = -1, param3 = -1, param4 = -1, param5 = -1, param7 = -1;
        String param1 = null;
        String param2 = null;
        String param6 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM masters");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getString(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            param6 = rs.getString(7);
            param7 = rs.getInt(8);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7);
        }
        System.out.println();
    }

    public static void getCharacters(Connection connection) throws SQLException {
        int param0 = -1, param2 = -1, param3 = -1, param4 = -1, param5 = -1, param6 = -1;
        String param1 = null;
        String param7 = null;
        String param8 = null;
        String param9 = null;
        String param10 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM characters");

        while (rs.next()) {
            param0 = rs.getInt(1);
            param1 = rs.getString(2);
            param2 = rs.getInt(3);
            param3 = rs.getInt(4);
            param4 = rs.getInt(5);
            param5 = rs.getInt(6);
            param6 = rs.getInt(7);
            param7 = rs.getString(8);
            param8 = rs.getString(9);
            param9 = rs.getString(10);
            param10 = rs.getString(11);
            System.out.println(param0 + " | " + param1 + " | " + param2 + " | " + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7 + " | " + param8 + " | " + param9 + " | " + param10);
        }
        System.out.println();
    }

    public static void getMoscowPlayers(Connection connection) throws SQLException {
        String columnName0 = "id";
        String columnName1 = "name";
        String columnName2 = "age";
        String columnName3 = "experience";
        String columnName4 = "games_count";
        String columnName5 = "city";
        String columnName6 = "gender";
        String columnName7 = "table_id";

        String param0 = null;
        String param1 = null;
        String param2 = null;
        String param3 = null;
        String param4 = null;
        String param5 = null;
        String param6 = null;
        String param7 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM players WHERE city = 'Москва'");

        while (rs.next()) {
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getString(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getString(columnName7);

            System.out.println(param0 + "  |  " + param1 + " | " + param2 + "  |  " + param3 + "  |  " + param4 +
                    " | " + param5 + "  |  " + param6 + " | " + param7);
        }
        System.out.println();
    }

    public static void getSessionsInfo(Connection connection, int hours) throws SQLException {
        if (hours < 0) return;
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT players.name, players.city, tables.difficulty, tables.id " +
                        "FROM players " +
                        "JOIN tables ON players.table_id = tables.id " +
                        "WHERE tables.session_hours > ? " +
                        "ORDER BY table_id;");
        statement.setInt(1, hours);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " + rs.getString(3) + " | " + rs.getString(4));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
        System.out.println();
    }

    public static void ChangeStatus(Connection connection, String status, int id) throws SQLException {
        if (id <= 0 || status == null || status.isBlank()) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE tables SET status = ? WHERE tables.id = ?;");
        statement.setString(1, status);
        statement.setInt(2, id);
        statement.executeUpdate();
        System.out.printf("Status of table %s updated! \n", id);
        getTables(connection);
        System.out.println();
    }

    public static void getProMasters(Connection connection) throws SQLException {
        String columnName0 = "id";
        String columnName1 = "name";
        String columnName2 = "gender";
        String columnName3 = "experience";
        String columnName4 = "age";
        String columnName5 = "games_count";
        String columnName6 = "city";
        String columnName7 = "table_id";

        String param0 = null;
        String param1 = null;
        String param2 = null;
        String param3 = null;
        String param4 = null;
        String param5 = null;
        String param6 = null;
        String param7 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM masters" +
                " WHERE experience >= 3 AND games_count >= 250 " +
                "ORDER BY ID");

        while (rs.next()) {
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getString(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getString(columnName7);

            System.out.println(param0 + "  |  " + param1 + " | " + param2 + "  |  " + param3 + "  |  " + param4 +
                    " | " + param5 + "  |  " + param6 + " | " + param7);
        }
        System.out.println();
    }

    public static void updateHealth(Connection connection, int hp, int id) throws SQLException {
        if (id <= 0 || hp < 0) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE characters SET hp = ?" +
                " WHERE characters.player_id = ?;");
        statement.setInt(1, hp);
        statement.setInt(2, id);
        statement.executeUpdate();
        System.out.printf("HP of player %s updated! \n", id);
        getCharacters(connection);
        System.out.println();
    }

    public static void createNewTable(Connection connection, int id, int count, int masterId,
                                      int hours, int difficulty, String status) throws SQLException {
        if (count < 0 || masterId <= 0 || hours < 0 || difficulty < 0 ||
                status == null || status.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("INSERT INTO tables (id," +
                " players_count, master_id, session_hours, difficulty, status)" +
                " VALUES (?, ?, ?, ?, ?, ? )");
        statement.setInt(1, id);
        statement.setInt(2, count);
        statement.setInt(3, masterId);
        statement.setInt(4, hours);
        statement.setInt(5, difficulty);
        statement.setString(6, status);
        statement.executeUpdate();
        System.out.println("Added one table with id %s");
        getTables(connection);
        System.out.println();
    }

    public static void deleteCharacter(Connection connection, int id) throws SQLException {
        if (id <= 0) return;
        PreparedStatement statement = connection.prepareStatement("DELETE FROM characters" +
                " WHERE player_id = ?");
        statement.setInt(1, id);
        statement.executeUpdate();
        System.out.printf("Character of player %s was deleted! \n", id);
        getCharacters(connection);
        System.out.println();
    }

    public static void getCharactersClassed(Connection connection) throws SQLException {
        String columnName0 = "player_id";
        String columnName1 = "name";
        String columnName2 = "age";
        String columnName3 = "height";
        String columnName4 = "hp";
        String columnName5 = "armor";
        String columnName6 = "attack";
        String columnName7 = "race";
        String columnName8 = "gender";
        String columnName9 = "class";
        String columnName10 = "weapon";

        String param0 = null;
        String param1 = null;
        String param2 = null;
        String param3 = null;
        String param4 = null;
        String param5 = null;
        String param6 = null;
        String param7 = null;
        String param8 = null;
        String param9 = null;
        String param10 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM characters" +
                " WHERE class = 'Лучник' " +
                "ORDER BY player_id");

        while (rs.next()) {
            param0 = rs.getString(columnName0);
            param1 = rs.getString(columnName1);
            param2 = rs.getString(columnName2);
            param3 = rs.getString(columnName3);
            param4 = rs.getString(columnName4);
            param5 = rs.getString(columnName5);
            param6 = rs.getString(columnName6);
            param7 = rs.getString(columnName7);
            param8 = rs.getString(columnName8);
            param9 = rs.getString(columnName9);
            param10 = rs.getString(columnName10);

            System.out.println(param0 + "  |  " + param1 + " | " + param2 + "  |  " + param3 + "  |  " + param4 +
                    " | " + param5 + "  |  " + param6 + " | " + param7 + "  |  " + param8 + " | " + param9 +
                    "  |  " + param10);
        }
        System.out.println();
    }

    public static void getCharactersPlayers(Connection connection, String gender) throws SQLException {
        if (gender == null || gender.isBlank()) return;
        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT players.name, players.age, characters.name, characters.age, characters.gender, " +
                        "characters.class FROM players " +
                        "LEFT JOIN characters ON players.id = characters.player_id " +
                        "WHERE characters.gender = ? " +
                        "ORDER BY players.id;");
        statement.setString(1, gender);
        ResultSet rs = statement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + " | " + rs.getString(2) + " | " +
                    rs.getString(3) + " | " + rs.getString(4) + " | " + rs.getString(5) +
                    " | " + rs.getString(6));
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
        System.out.println();
    }

    public static void updateDifficulty(Connection connection, int difficulty, int id) throws SQLException {
        if (difficulty < 0 || id <= 0) return;
        PreparedStatement statement = connection.prepareStatement("UPDATE tables SET difficulty = ?" +
                " WHERE tables.id = ?;");
        statement.setInt(1, difficulty);
        statement.setInt(2, id);
        statement.executeUpdate();
        System.out.printf("Difficulty on table %s has been changed to %s! \n", id, difficulty);
        getTables(connection);
        System.out.println();
    }
}