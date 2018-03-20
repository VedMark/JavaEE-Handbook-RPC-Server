import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import thrift.J2EeHandbookService;
import thrift.JavaEETechnology;
import thrift.JavaEETechnologyVersions;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

class J2EeHandbookHandler implements J2EeHandbookService.Iface {
    private static final Logger log = LogManager.getLogger(J2EeHandbookService.class);

    @Override
    public List<JavaEETechnology> getAllTechnologies() {
        String SQL;
        ResultSet rs;
        List<JavaEETechnology> list = new ArrayList<JavaEETechnology>();
        try {
            SQL = "SELECT *\n" +
                    "FROM java_technologies INNER JOIN used_versions\n" +
                    "ON java_technologies.versions = used_versions.used_versions_id;";
            try {
                Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            String url = ResourceBundle.getBundle("config").getString("db.url");
            String user = ResourceBundle.getBundle("config").getString("db.user");
            String password = ResourceBundle.getBundle("config").getString("db.password");

            Statement stmt = DriverManager.getConnection(url, user, password).createStatement();
            rs = stmt.executeQuery(SQL);

            while (rs.next()) {
                list.add(fromResultSetToJavaEETechnologyObject(rs));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return list;
    }

    @Override
    public void addTechnology(JavaEETechnology technology) {
        String SQL;
        PreparedStatement preparedStatement;
        try {
            SQL = "INSERT INTO used_versions(java_4, java_5, java_6, java_7, java_8) \n" +
                    "VALUES(?, ?, ?, ?, ?)\n" +
                    "ON DUPLICATE KEY UPDATE java_4=?, java_5=?, java_6=?, java_7=?, java_8=?;";

            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            String url = ResourceBundle.getBundle("config").getString("db.url");
            String user = ResourceBundle.getBundle("config").getString("db.user");
            String password = ResourceBundle.getBundle("config").getString("db.password");

            preparedStatement = DriverManager.getConnection(url, user, password).prepareStatement(SQL);
            preparedStatement.setString(1, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(2, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(3, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(4, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(5, technology.getVersions().getVersionForJava8());
            preparedStatement.setString(6, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(7, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(8, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(9, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(10, technology.getVersions().getVersionForJava8());
            preparedStatement.executeUpdate();

            SQL = "INSERT INTO java_technologies(tech_name, versions, description)\n" +
                    "VALUES(?,\n" +
                    "(SELECT used_versions_id\n" +
                    "FROM used_versions\n" +
                    "WHERE java_4=? AND java_5=? AND java_6=? AND java_7=? AND java_8=?), ?)\n" +
                    "ON DUPLICATE KEY UPDATE tech_name=?;";
            preparedStatement = DriverManager.getConnection(url, user, password).prepareStatement(SQL);
            preparedStatement.setString(1, technology.getName());
            preparedStatement.setString(2, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(3, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(4, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(5, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(6, technology.getVersions().getVersionForJava8());
            preparedStatement.setString(7, technology.getDescription());
            preparedStatement.setString(8, technology.getName());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void removeTechnology(JavaEETechnology technology) {
        String SQL;
        PreparedStatement preparedStatement;
        try {
            SQL = "DELETE FROM java_technologies\n" +
                    "WHERE tech_id=?;";

            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            String url = ResourceBundle.getBundle("config").getString("db.url");
            String user = ResourceBundle.getBundle("config").getString("db.user");
            String password = ResourceBundle.getBundle("config").getString("db.password");

            preparedStatement = DriverManager.getConnection(url, user, password).prepareStatement(SQL);
            preparedStatement.setInt(1, technology.getId());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void updateTechnology(JavaEETechnology technology) {
        String SQL;
        PreparedStatement preparedStatement;
        try {
            SQL = "INSERT INTO used_versions(java_4, java_5, java_6, java_7, java_8) \n" +
                    "VALUES(?, ?, ?, ?, ?)\n" +
                    "ON DUPLICATE KEY UPDATE java_4=?, java_5=?, java_6=?, java_7=?, java_8=?;";

            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            String url = ResourceBundle.getBundle("config").getString("db.url");
            String user = ResourceBundle.getBundle("config").getString("db.user");
            String password = ResourceBundle.getBundle("config").getString("db.password");

            preparedStatement = DriverManager.getConnection(url, user, password).prepareStatement(SQL);
            preparedStatement.setString(1, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(2, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(3, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(4, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(5, technology.getVersions().getVersionForJava8());
            preparedStatement.setString(6, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(7, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(8, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(9, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(10, technology.getVersions().getVersionForJava8());
            preparedStatement.executeUpdate();

            SQL = "UPDATE java_technologies\n" +
                    "SET tech_name=?, versions=(SELECT used_versions_id\n" +
                    "FROM used_versions\n" +
                    "WHERE java_4=? AND java_5=? AND java_6=? AND java_7=? AND java_8=?),\n" +
                    "description=?\n" +
                    "WHERE tech_id=?;";
            preparedStatement = DriverManager.getConnection(url, user, password).prepareStatement(SQL);
            preparedStatement.setString(1, technology.getName());
            preparedStatement.setString(2, technology.getVersions().getVersionForJava4());
            preparedStatement.setString(3, technology.getVersions().getVersionForJava5());
            preparedStatement.setString(4, technology.getVersions().getVersionForJava6());
            preparedStatement.setString(5, technology.getVersions().getVersionForJava7());
            preparedStatement.setString(6, technology.getVersions().getVersionForJava8());
            preparedStatement.setString(7, technology.getDescription());
            preparedStatement.setInt(8, technology.getId());
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private JavaEETechnology fromResultSetToJavaEETechnologyObject(ResultSet rs) throws SQLException {
        JavaEETechnology technology = new JavaEETechnology();
        technology.setId(rs.getInt("tech_id"));
        technology.setName(rs.getString("tech_name"));
        technology.setVersions(new JavaEETechnologyVersions());
        technology.getVersions().setVersionForJava4(rs.getString("java_4"));
        technology.getVersions().setVersionForJava5(rs.getString("java_5"));
        technology.getVersions().setVersionForJava6(rs.getString("java_6"));
        technology.getVersions().setVersionForJava7(rs.getString("java_7"));
        technology.getVersions().setVersionForJava8(rs.getString("java_8"));
        technology.setDescription(rs.getString("description"));

        return technology;
    }
}
