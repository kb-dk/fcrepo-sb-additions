package dk.statsbiblioteket.metadatarepository.fieldsearch;

import org.fcrepo.server.Server;
import org.fcrepo.server.errors.ConnectionPoolNotFoundException;
import org.fcrepo.server.errors.ModuleInitializationException;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.search.FieldSearchQuery;
import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.FieldSearchSQLImpl;
import org.fcrepo.server.search.FieldSearchSQLModule;
import org.fcrepo.server.storage.ConnectionPool;
import org.fcrepo.server.storage.ConnectionPoolManager;
import org.fcrepo.server.storage.DOManager;
import org.fcrepo.server.storage.DOReader;
import org.fcrepo.server.utilities.SQLUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SBFieldSearchModule extends FieldSearchSQLModule {
    private static final Logger logger = LoggerFactory.getLogger(SBFieldSearchModule.class);
    private FieldSearchSQLImpl m_wrappedFieldSearch;

    public SBFieldSearchModule(Map params, Server server, String role) throws ModuleInitializationException {
        super(params, server, role);
    }

    @Override
    public void postInitModule() throws ModuleInitializationException { //Duplicated from super as we need to override the m_wrappedFieldSearch
        int maxResults = getMaxResults();
        int maxSecondsPerSession = getMaxSecondsPerSession();
        boolean indexDCFields = getIndexDCFields();
        ConnectionPool cPool = getConnectionPool();
        createDoIdentifierTable(cPool);
        DOManager doManager = getDoManager();
        m_wrappedFieldSearch = new SBFieldSearchSQLImpl(cPool, doManager, maxResults, maxSecondsPerSession, indexDCFields);
    }

    private DOManager getDoManager() throws ModuleInitializationException {
        //
        // get the doManager
        //
        DOManager doManager = (DOManager) getServer().getModule("org.fcrepo.server.storage.DOManager");
        if (doManager == null) {
            throw new ModuleInitializationException("DOManager module was required, but apparently has " + "not been loaded.",
                    getRole());
        }
        return doManager;
    }

    /**
     * Create the doIdentifier table
     * @param cPool the connection pool
     * @throws ModuleInitializationException
     */
    private void createDoIdentifierTable(ConnectionPool cPool) throws ModuleInitializationException {
    /*Create the table, as this is not created by Fedora default*/
        try {
            String dbSpec = "dk/statsbiblioteket/metadatarepository/SBFieldSearch.dbspec";
            InputStream specIn = this.getClass().getClassLoader().getResourceAsStream(dbSpec);
            if (specIn == null) {
                throw new IOException("Cannot find required " + "resource: " +
                                      dbSpec);
            }
            SQLUtility.createNonExistingTables(cPool, specIn);
        } catch (Exception e) {
            throw new ModuleInitializationException("Error while attempting to " +
                                                    "check for and create non-existing table(s): " +
                                                    e.getClass().getName() + ": " + e.getMessage(), getRole(), e);
        }
    }

    private ConnectionPool getConnectionPool() throws ModuleInitializationException {
        //
        // get connectionPool from ConnectionPoolManager
        //
        ConnectionPoolManager cpm = (ConnectionPoolManager) getServer().getModule(
                "org.fcrepo.server.storage.ConnectionPoolManager");
        if (cpm == null) {
            throw new ModuleInitializationException("ConnectionPoolManager module was required, but apparently has " + "not been loaded.",
                    getRole());
        }
        String cPoolName = getParameter("connectionPool");
        ConnectionPool cPool = null;
        try {
            if (cPoolName == null) {
                logger.debug("connectionPool unspecified; using default from " + "ConnectionPoolManager.");
                cPool = cpm.getPool();
            } else {
                logger.debug("connectionPool specified: " + cPoolName);
                cPool = cpm.getPool(cPoolName);
            }
        } catch (ConnectionPoolNotFoundException cpnfe) {
            throw new ModuleInitializationException("Could not find requested " + "connectionPool.", getRole());
        }
        return cPool;
    }

    private boolean getIndexDCFields() throws ModuleInitializationException {
        //
        // get indexDCFields parameter (default to true if unspecified)
        //
        boolean indexDCFields = true;
        String indexDCFieldsValue = getParameter("indexDCFields");
        if (indexDCFieldsValue != null) {
            String val = indexDCFieldsValue.trim().toLowerCase();
            if (val.equals("false") || val.equals("no")) {
                indexDCFields = false;
            } else if (!val.equals("true") && !val.equals("yes")) {
                throw new ModuleInitializationException("indexDCFields param " + "was not a boolean", getRole());
            }
        }
        return indexDCFields;
    }

    private int getMaxSecondsPerSession() throws ModuleInitializationException {
        //
        // get and validate maxSecondsPerSession
        //
        if (getParameter("maxSecondsPerSession") == null) {
            throw new ModuleInitializationException("maxSecondsPerSession parameter must be specified.", getRole());
        }
        int maxSecondsPerSession = 0;
        try {
            maxSecondsPerSession = Integer.parseInt(getParameter("maxSecondsPerSession"));
            if (maxSecondsPerSession < 1) {
                throw new NumberFormatException("");
            }
        } catch (NumberFormatException nfe) {
            throw new ModuleInitializationException("maxSecondsPerSession must be a positive integer.", getRole());
        }
        return maxSecondsPerSession;
    }

    private int getMaxResults() throws ModuleInitializationException {
        //
        // get and validate maxResults
        //
        if (getParameter("maxResults") == null) {
            throw new ModuleInitializationException("maxResults parameter must be specified.", getRole());
        }
        int maxResults = 0;
        try {
            maxResults = Integer.parseInt(getParameter("maxResults"));
            if (maxResults < 1) {
                throw new NumberFormatException("");
            }
        } catch (NumberFormatException nfe) {
            throw new ModuleInitializationException("maxResults must be a positive integer.", getRole());
        }
        return maxResults;
    }

    @Override
    public void update(DOReader reader) throws ServerException {
        m_wrappedFieldSearch.update(reader);
    }

    @Override
    public boolean delete(String pid) throws ServerException {
        return m_wrappedFieldSearch.delete(pid);
    }

    @Override
    public FieldSearchResult findObjects(String[] resultFields, int maxResults, FieldSearchQuery query) throws
                                                                                                        ServerException {
        return m_wrappedFieldSearch.findObjects(resultFields, maxResults, query);
    }

    @Override
    public FieldSearchResult resumeFindObjects(String sessionToken) throws ServerException {
        return m_wrappedFieldSearch.resumeFindObjects(sessionToken);
    }
}
