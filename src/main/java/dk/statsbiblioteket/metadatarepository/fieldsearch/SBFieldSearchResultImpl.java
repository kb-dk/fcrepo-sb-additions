package dk.statsbiblioteket.metadatarepository.fieldsearch;

import org.fcrepo.server.search.FieldSearchResult;
import org.fcrepo.server.search.ObjectFields;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * The SearchResult class for a result from the doIdentifiers table
 */
public class SBFieldSearchResultImpl implements FieldSearchResult {

    List<ObjectFields> pids;
    long cursor = 0;

    private long listOffset;
    private long completeSize;

    public SBFieldSearchResultImpl(List<String> pids, long cursor, long listOffset, long completeSize) {

        this.pids = convert(pids);
        this.cursor = cursor;
        this.listOffset = listOffset;
        this.completeSize = completeSize;
    }

    public SBFieldSearchResultImpl(ResultSet m_resultSet) throws SQLException {
        listOffset = 0;
        pids = new ArrayList<>();
        while (m_resultSet.next())  {
            ObjectFields element = new ObjectFields();
            element.setPid(m_resultSet.getString("pid"));
            pids.add(element);
        }
        completeSize = pids.size();
        listOffset = 0;
    }

    private List<ObjectFields> convert(List<String> pids) {
        ArrayList<ObjectFields> result = new ArrayList<>(pids.size());
        for (String pid : pids) {
            ObjectFields element = new ObjectFields();
            element.setPid(pid);
            result.add(element);
        }
        return result;
    }

    @Override
    public List<ObjectFields> objectFieldsList() {
        return pids;
    }

    @Override
    public String getToken() {
        return null;
    }

    @Override
    public long getCursor() {
        return cursor;
    }

    @Override
    public long getCompleteListSize() {
        return pids.size();
    }

    @Override
    public Date getExpirationDate() {
        return null;
    }
}
