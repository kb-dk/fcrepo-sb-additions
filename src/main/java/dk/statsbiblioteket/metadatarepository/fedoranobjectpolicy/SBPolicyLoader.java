package dk.statsbiblioteket.metadatarepository.fedoranobjectpolicy;

import com.sun.xacml.AbstractPolicy;
import org.fcrepo.server.errors.ServerException;
import org.fcrepo.server.security.PolicyParser;
import org.fcrepo.server.security.impl.SimplePolicyLoader;
import org.fcrepo.server.storage.RepositoryReader;

/**
 * Created with IntelliJ IDEA.
 * User: abr
 * Date: 6/11/13
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class SBPolicyLoader extends SimplePolicyLoader {
    public SBPolicyLoader(RepositoryReader repoReader) {
        super(repoReader);
    }

    @Override
    protected AbstractPolicy loadObjectPolicy(PolicyParser policyParser, String pid, boolean validate) throws ServerException {
        return null;
    }
}
