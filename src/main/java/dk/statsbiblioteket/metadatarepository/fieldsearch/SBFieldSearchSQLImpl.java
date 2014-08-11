package dk.statsbiblioteket.metadatarepository.fieldsearch;

import org.fcrepo.server.errors.ObjectIntegrityException;
import org.fcrepo.server.errors.RepositoryConfigurationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.errors.StorageDeviceException;
import org.fcrepo.server.errors.StreamIOException;
import org.fcrepo.server.errors.UnrecognizedFieldException;
import org.fcrepo.server.search.Condition;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.FieldSearchSQLImpl;
import org.fcrepo.server.search.Operator;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.storage.RepositoryReader;
import org.fcrepo.server.storage.types.Datastream;
import org.fcrepo.server.utilities.DCField;
import org.fcrepo.server.utilities.DCFields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SBFieldSearchSQLImpl extends FieldSearchSQLImpl {

    private static final Logger logger = LoggerFactory.getLogger(SBFieldSearchSQLImpl.class);
    private final ConnectionPool m_cPool;
    private final RepositoryReader m_repoReader;
    private final int m_maxResults;
    private final int m_maxSecondsPerSession;

    public SBFieldSearchSQLImpl(ConnectionPool cPool, RepositoryReader repoReader, int maxResults,
                                int maxSecondsPerSession) {
        super(cPool, repoReader, maxResults, maxSecondsPerSession);
        m_cPool = cPool;
        m_repoReader = repoReader;
        m_maxResults = maxResults;
        m_maxSecondsPerSession = maxSecondsPerSession;
    }

    public SBFieldSearchSQLImpl(ConnectionPool cPool, RepositoryReader repoReader, int maxResults,
                                int maxSecondsPerSession, boolean indexDCFields) {
        super(cPool, repoReader, maxResults, maxSecondsPerSession, indexDCFields);
        m_cPool = cPool;
        m_repoReader = repoReader;
        m_maxResults = maxResults;
        m_maxSecondsPerSession = maxSecondsPerSession;
    }

    @Override
    public void update(DOReader reader) throws ServerException {
        super.update(reader);
        logger.debug("Entering update(DOReader)");
        String pid = reader.GetObjectPID();
        Connection conn = null;
        PreparedStatement delete = null;
        try {
            conn = m_cPool.getReadWriteConnection();
            // do dc stuff if needed
            Datastream dcmd;
            try {
                dcmd = reader.GetDatastream("DC", null);
            } catch (ClassCastException cce) {
                throw new ObjectIntegrityException("Object " + pid + " has a DC datastream, but it's not inline XML.");
            }
            if (dcmd != null) {
                InputStream in = dcmd.getContentStream();
                DCFields dc = new DCFields(in);
                delete = conn.prepareStatement("DELETE FROM doIdentifiers WHERE pid=?");
                delete.setString(1, pid);
                delete.executeUpdate();
                final List<DCField> identifiers = dc.identifiers();
                for (DCField identifier : identifiers) {
                    PreparedStatement insert = conn.prepareStatement(
                            "INSERT INTO doIdentifiers (pid, dcIdentifier) VALUES (?, ?)");
                    insert.setString(1, pid);
                    insert.setString(2, identifier.getValue());
                    insert.executeUpdate();
                }
                logger.debug("Formulating SQL and inserting/updating WITH DC...");
            }
        } catch (SQLException sqle) {
            throw new StorageDeviceException("Error attempting FieldSearch " + "update of " + pid, sqle);
        } finally {
            try {
                if (delete != null) {
                    delete.close();
                }
                if (conn != null) {
                    m_cPool.free(conn);
                }
            } catch (SQLException sqle2) {
                throw new StorageDeviceException("Error closing statement " + "while attempting update of object" + sqle2
                        .getMessage());
            } finally {
                logger.debug("Exiting update(DOReader)");
            }
        }
    }

    @Override
    public boolean delete(String pid) throws ServerException {
        logger.debug("Entering update(DOReader)");
        Connection conn = null;
        PreparedStatement st = null;
        try {
            conn = m_cPool.getReadWriteConnection();
            st = conn.prepareStatement("DELETE FROM doIdentifiers WHERE pid=?");
            st.setString(1, pid);
            st.executeUpdate();
            return super.delete(pid);
        } catch (SQLException sqle) {
            throw new StorageDeviceException("Error attempting FieldSearch " + "update of " + pid, sqle);
        } finally {
            try {
                if (st != null) {
                    st.close();
                }
                if (conn != null) {
                    m_cPool.free(conn);
                }
            } catch (SQLException sqle2) {
                throw new StorageDeviceException("Error closing statement " + "while attempting update of object" + sqle2
                        .getMessage());
            } finally {
                logger.debug("Exiting update(DOReader)");
            }
        }
    }

    @Override
    public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws
                                                                                                        UnrecognizedFieldException,
                                                                                                        ObjectIntegrityException,
                                                                                                        RepositoryConfigurationException,
                                                                                                        StreamIOException,
                                                                                                        ServerException,
                                                                                                        StorageDeviceException {
        List<Condition> conditions = query.getConditions();

        /*If
        * only one condition
        * condition is on dcIdentifier
        * operator is EQ
        * value is value*/
        if (resultFields.length == 1 && resultFields[0].equals("pid")) {
            if (conditions.size() == 1) {
                Condition condition = conditions.get(0);
                if (condition.getProperty().equals("identifier")) {
                    if (condition.getOperator() == Operator.EQUALS || (condition.getOperator() == Operator.CONTAINS && !condition
                            .getValue()
                            .contains("*") && !condition.getValue().contains("?"))) {
                        PreparedStatement m_statement = null;
                        ResultSet m_resultSet = null;
                        Connection conn = null;
                        try {
                            conn = m_cPool.getReadOnlyConnection();
                            m_statement = conn.prepareStatement(
                                    "SELECT doIdentifiers.pid FROM doIdentifiers where doIdentifiers.dcIdentifier=?");
                            m_statement.setString(1, condition.getValue());
                            m_resultSet = m_statement.executeQuery();
                            return new SBFieldSearchResultImpl(m_resultSet);
                        } catch (SQLException e) {
                            throw new StorageDeviceException("Error querying sql db: " + e.getMessage(), e);
                        } finally {
                            try {
                                if (m_resultSet != null) {
                                    m_resultSet.close();
                                }
                                if (m_statement != null) {
                                    m_statement.close();
                                }
                                if (conn != null) {
                                    m_cPool.free(conn);
                                }
                            } catch (SQLException e) {
                                logger.warn("SQL error during failed query cleanup", e);
                            }
                        }
                    }
                }
            }
        }
        return super.findObjects(resultFields, maxResults, query);
    }
}
