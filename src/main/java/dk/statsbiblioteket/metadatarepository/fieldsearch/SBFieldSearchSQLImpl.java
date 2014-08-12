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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SBFieldSearchSQLImpl extends FieldSearchSQLImpl {

    private static final Logger logger = LoggerFactory.getLogger(SBFieldSearchSQLImpl.class);
    private final ConnectionPool m_cPool;

    public SBFieldSearchSQLImpl(ConnectionPool cPool, RepositoryReader repoReader, int maxResults,
                                int maxSecondsPerSession) {
        super(cPool, repoReader, maxResults, maxSecondsPerSession);
        m_cPool = cPool;
    }

    public SBFieldSearchSQLImpl(ConnectionPool cPool, RepositoryReader repoReader, int maxResults,
                                int maxSecondsPerSession, boolean indexDCFields) {
        super(cPool, repoReader, maxResults, maxSecondsPerSession, indexDCFields);
        m_cPool = cPool;
    }

    /**
     * This Method updates the doIdentifier table and calls FieldSearchSQLImpl to ensure that the normal doField tables
     * are
     * up2date
     *
     * @param reader the object reader
     *
     * @throws ServerException if anything failed
     */
    public void update(DOReader reader) throws ServerException {
        super.update(reader);
        logger.debug("Entering update(DOReader)");
        String pid = reader.GetObjectPID();
        Connection conn = null;
        try { //Try for getting the connection
            conn = m_cPool.getReadWriteConnection();
            try { //try for the transaction
                conn.setAutoCommit(false);//Start Transaction
                final List<DCField> identifiers = getIdentifiers(reader);
                try (PreparedStatement delete = conn.prepareStatement("DELETE FROM doIdentifiers WHERE pid=?")) {
                    delete.setString(1, pid);
                    delete.executeUpdate();
                }
                for (DCField identifier : identifiers) {
                    try (PreparedStatement insert = conn.prepareStatement(
                            "INSERT INTO doIdentifiers (pid, dcIdentifier) VALUES (?, ?)")) {
                        insert.setString(1, pid);
                        insert.setString(2, identifier.getValue());
                        insert.executeUpdate();
                    }
                }
                logger.debug("Formulating SQL and inserting/updating WITH DC...");
                conn.commit();
            } catch (Exception e) {
                rollback(conn);
                throw new StorageDeviceException("Error attempting FieldSearch " + "update of " + pid, e);
            } finally {
                conn.setAutoCommit(true);//restore autocommit
                m_cPool.free(conn);
            }
        } catch (SQLException e) {
            throw new StorageDeviceException("Error attempting FieldSearch " + "update of " + pid, e);
        }
    }

    /**
     * Utility method for reading the identifiers from a dc datastream
     * @param reader the object reader
     * @return a list of dc fields, for the identifiers
     * @throws ServerException if the server failed
     * @throws IOException if the reading failed
     */
    private List<DCField> getIdentifiers(DOReader reader) throws ServerException, IOException {
        String pid = reader.getOwnerId();
        List<DCField> identifiers = new ArrayList<>();
        try { //try for getting the DC datastream
            Datastream dcDatastream;
            dcDatastream = reader.GetDatastream("DC", null);
            if (dcDatastream != null) {
                try (InputStream in = dcDatastream.getContentStream()) {
                    DCFields dc = new DCFields(in);
                    identifiers = dc.identifiers();
                }
            }
        } catch (ClassCastException cce) {
            throw new ObjectIntegrityException("Object " + pid + " has a DC datastream, but it's not inline XML.");
        }
        return identifiers;
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e1) {
            //Ignored
        }
    }

    /**
     * Delete the object from the doIdentifiers table and then call super
     * @param pid the pid to delete
     * @return true if the delete succeeded
     * @throws ServerException if the delete failed
     */
    @Override
    public boolean delete(String pid) throws ServerException {
        logger.debug("Entering delete(DOReader)");
        Connection conn;
        try {
            conn = m_cPool.getReadWriteConnection();
            try (PreparedStatement st = conn.prepareStatement("DELETE FROM doIdentifiers WHERE pid=?")) {
                st.setString(1, pid);
                st.executeUpdate();
                return super.delete(pid);
            } finally {
                m_cPool.free(conn);
            }
        } catch (SQLException sqle) {
            throw new StorageDeviceException("Error attempting FieldSearch " + "update of " + pid, sqle);
        } finally {
            logger.debug("Exiting update(DOReader)");
        }
    }

    /**
     * If you search with a Condition query, with just one condition, namely that the the identifier should be EQUALS some value
     * and you only use the resultField pid, perform a search in the doIdentifiers table. Otherwise, do a normal fieldSearch
     * @param resultFields the resultFields
     * @param maxResults maxResults, not used here
     * @param query the query
     * @return the search Result
     * @throws UnrecognizedFieldException If you use an unregnized field in the query conditions
     * @throws ObjectIntegrityException if the object cannot be read
     * @throws RepositoryConfigurationException if the configuration is wrong
     * @throws StreamIOException what it says on the tin
     * @throws ServerException general exception
     * @throws StorageDeviceException if the database fails
     */
    @Override
    public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws
                                                                                                        UnrecognizedFieldException,
                                                                                                        ObjectIntegrityException,
                                                                                                        RepositoryConfigurationException,
                                                                                                        StreamIOException,
                                                                                                        ServerException,
                                                                                                        StorageDeviceException {

        if (resultFields.length == 1 && resultFields[0].equals("pid")) {  //result is only pids
            List<Condition> conditions = query.getConditions();
            if (conditions != null && conditions.size() == 1) { // and only one condition
                Condition condition = conditions.get(0);
                if (condition.getProperty().equals("identifier")) { //and this is a condition on dcIdentifier
                    if (condition.getOperator() == Operator.EQUALS) { //and the condition is equals
                        return searchUsingSBFieldSearch(condition);
                    }
                }
            }
        } // If any of the conditions failed, forward to the normal fieldSearch
        return super.findObjects(resultFields, maxResults, query);
    }

    private FieldSearchResult searchUsingSBFieldSearch(Condition condition) throws StorageDeviceException {
        try {
            Connection conn = m_cPool.getReadOnlyConnection();
            try (PreparedStatement m_statement = conn.prepareStatement(
                    "SELECT doIdentifiers.pid FROM doIdentifiers where doIdentifiers.dcIdentifier=?")) {
                m_statement.setString(1, condition.getValue());
                try (ResultSet m_resultSet = m_statement.executeQuery()) {
                    return new SBFieldSearchResultImpl(m_resultSet);
                }
            } finally {
                m_cPool.free(conn);
            }
        } catch (SQLException e) {
            throw new StorageDeviceException("Error querying sql db: " + e.getMessage(), e);
        }
    }
}
