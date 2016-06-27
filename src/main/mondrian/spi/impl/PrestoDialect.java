package mondrian.spi.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import mondrian.rolap.SqlStatement;
import mondrian.rolap.SqlStatement.Type;

public class PrestoDialect extends JdbcDialectImpl {

	public static final JdbcDialectFactory FACTORY = new JdbcDialectFactory(PrestoDialect.class,
			DatabaseProduct.Presto);

	/**
	 * Creates a PrestoDialect.
	 *
	 * @param connection
	 *            Connection
	 *
	 * @throws SQLException
	 *             on error
	 */
	public PrestoDialect(Connection connection) throws SQLException {
		super(connection);
	}

	@Override
	public Type getType(ResultSetMetaData metaData, int columnIndex) throws SQLException {
		// Presto's driver doesn't like getInt on a boolean.
		// Treat it as an Object.
		switch (metaData.getColumnType(columnIndex + 1)) {
		case Types.BOOLEAN:
			return SqlStatement.Type.OBJECT;
		default:
			return super.getType(metaData, columnIndex);
		}
	}

	@Override
	protected String deduceIdentifierQuoteString(DatabaseMetaData databaseMetaData) {
		return "\"";
	}

	public String generateInline(List<String> columnNames, List<String> columnTypes, List<String[]> valueList) {
		return generateInlineGeneric(columnNames, columnTypes, valueList, null, false);
	}

	@Override
	public String generateOrderByNulls(String expr, boolean ascending, boolean collateNullsLast) {
		return generateOrderByNullsAnsi(expr, ascending, collateNullsLast);
	}
}
