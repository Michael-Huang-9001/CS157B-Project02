import javax.sql.DataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

	private static boolean RESET_DATABASE = true;

	// Setting up connection and statements.
	private static DataSource ds = DataSourceFactory.getMySQLDataSource();
	private static PreparedStatement statement;
	private static Connection connection;

	private static int maxUserID;
	private static int maxItemID;
	private static int maxSalesID;

	public static void main(String[] args) {
		try {
			System.out.println("Establishing database connection.");
			connection = ds.getConnection();
			System.out.println("Connection established.");

			System.out.println("-----");

			// Connection connection = <your java.sql.Connection>
			ResultSet resultSet = connection.getMetaData().getCatalogs();

			// iterate each catalog in the ResultSet
			while (resultSet.next()) {
				// Get the database name, which is at position 1
				if (resultSet.getString(1).equals("project02")) {
					System.out.println("DB exists.");
					RESET_DATABASE = false;
					break;
				}
			}
			resultSet.close();

			// Make a new database if it dowsn't exist.
			if (RESET_DATABASE) {
				// Create database
				createDatabase();

				// Create tables
				createTables();

				maxUserID = 1;
				maxItemID = 1;
				maxSalesID = 1;

				System.out.println("Database and tables initialized without errors.");
			} else {
				statement = connection.prepareStatement("USE project02;");
				statement.executeUpdate();

				// Since MySQL loses auto increment's portability, we must keep
				// track of it on the Java level.
				statement = connection.prepareStatement("SELECT max(customer_ID) as customer_ID FROM users;");
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					maxUserID = resultSet.getInt("customer_ID");
					if (maxUserID == 0) {
						maxUserID = 1;
					}
					System.out.println("PK user_ID: " + maxUserID);
				}

				statement = connection.prepareStatement("SELECT max(item_ID) as item_ID FROM items;");
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					maxItemID = resultSet.getInt("item_ID");
					if (maxItemID == 0) {
						maxItemID = 1;
					}
					System.out.println("PK item_ID: " + maxItemID);
				}

				statement = connection.prepareStatement("SELECT max(sales_ID) as sales_ID FROM sales;");
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					maxSalesID = resultSet.getInt("sales_ID");
					if (maxSalesID == 0) {
						maxSalesID = 1;
					}
					System.out.println("PK sales_ID: " + maxSalesID);
				}
			}

			// java.sql.Date a = new java.sql.Date(System.currentTimeMillis());

			System.out.println("End transmission.");
			
		} catch (SQLException sql) {
			System.out.println("[SQL Error] " + sql.getMessage());
			sql.printStackTrace();
		} catch (Exception e) {
			System.out.println("[Other Error] " + e.getMessage());
		}
	}

	/**
	 * Create the database for project02.
	 * 
	 * @throws SQLException
	 *             if something goes wrong
	 */
	public static void createDatabase() throws SQLException {
		System.out.println("DROP database IF EXISTS project02;");
		statement = connection.prepareStatement("DROP database IF EXISTS project02;");
		statement.executeUpdate();
		System.out.println("CREATE database project02;");
		statement = connection.prepareStatement("CREATE database project02;");
		statement.executeUpdate();
		System.out.println("USE project02;");
		statement = connection.prepareStatement("USE project02;");
		statement.executeUpdate();
	}

	public static void createTables() throws SQLException {

		// Creating table users, USERS(customer_id, uNAME, BANNED);
		String table = "CREATE table users(" + "customer_ID INT NOT NULL," + "uNAME VARCHAR(15) NOT NULL,"
				+ "BANNED boolean default 0," + "PRIMARY KEY(customer_ID)" + ");";
		System.out.println(table);
		statement = connection.prepareStatement(table);
		statement.executeUpdate();

		// Creating table items, Items(Item_ID, Item_name, Cost, department);
		table = "CREATE table items(" + "item_ID INT NOT NULL," + "item_name VARCHAR(25) NOT NULL,"
				+ "cost double not null," + "department VARCHAR(20) DEFAULT NULL," + "PRIMARY KEY(item_id, department)"
				+ ");";
		System.out.println(table);
		statement = connection.prepareStatement(table);
		statement.executeUpdate();

		// Needed for foreign key on department.
		table = "CREATE index department on items(department);";
		System.out.println(table);
		statement = connection.prepareStatement(table);
		statement.executeUpdate();

		// Creating table sales, Sales(Sales_ID, customer_id, Item_ID,
		// SaleDate);
		table = "CREATE table sales(" + "sales_ID INT NOT NULL," + "customer_ID INT," + "item_ID INT,"
				+ "sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP," + "department VARCHAR(20) DEFAULT 'None',"
				+ "PRIMARY KEY(sales_ID),";
		table += "FOREIGN KEY(customer_ID) references users(customer_ID) ON DELETE SET NULL,";
		table += "FOREIGN KEY(item_ID) references items(item_ID) ON DELETE SET NULL,";
		table += "FOREIGN KEY(department) references items(department) ON DELETE SET NULL";
		table += ");";
		System.out.println(table);
		statement = connection.prepareStatement(table);
		statement.executeUpdate();
	}
}

class DataSourceFactory {
	// This references an external file db.properties containing the username,
	// password, and localhost port.
	public static DataSource getMySQLDataSource() {
		Properties props = new Properties();
		FileInputStream fis = null;
		MysqlDataSource mysqlDS = null;
		try {
			fis = new FileInputStream("db.properties");
			props.load(fis);
			mysqlDS = new MysqlDataSource();
			mysqlDS.setURL(props.getProperty("MYSQL_DB_URL"));
			mysqlDS.setUser(props.getProperty("MYSQL_DB_USERNAME"));
			mysqlDS.setPassword(props.getProperty("MYSQL_DB_PASSWORD"));
		} catch (IOException e) {
			System.out.println("db.properties is not found");
			e.printStackTrace();
		}
		return mysqlDS;
	}
}