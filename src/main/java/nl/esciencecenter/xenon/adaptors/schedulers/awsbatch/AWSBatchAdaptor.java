package nl.esciencecenter.xenon.adaptors.schedulers.awsbatch;

import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.schedulers.SchedulerAdaptor;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import nl.esciencecenter.xenon.schedulers.Scheduler;

public class AWSBatchAdaptor extends SchedulerAdaptor {
    /** The name of this adaptor */
    public static final String ADAPTOR_NAME = "awsbatch";

    /** A description of this adaptor */
    private static final String ADAPTOR_DESCRIPTION = "The S3 adaptor uses Apache JClouds to talk to s3 and others. To authenticate use PasswordCredential with access key id as username and secret access key as password";

    /** All our own properties start with this prefix. */
    public static final String PREFIX = SchedulerAdaptor.ADAPTORS_PREFIX + ADAPTOR_NAME + ".";

    /** The locations supported by this adaptor */
    private static final String[] ADAPTOR_LOCATIONS = new String[] { "region"};

    /** List of properties supported by this FTP adaptor */
    private static final XenonPropertyDescription[] VALID_PROPERTIES = new XenonPropertyDescription[] {};

    public AWSBatchAdaptor() {
        super(ADAPTOR_NAME, ADAPTOR_DESCRIPTION, ADAPTOR_LOCATIONS, VALID_PROPERTIES);
    }

    @Override
    public Scheduler createScheduler(String location, Credential credential, Map<String, String> properties) throws XenonException {
        AWSBatchClientBuilder builder = AWSBatchClientBuilder.standard();
        if (credential instanceof PasswordCredential) {
            PasswordCredential pwCred = (PasswordCredential) credential;
            AWSCredentials awsCredentials = new BasicAWSCredentials(pwCred.getUsername(), new String(pwCred.getPassword()));
            AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
            builder.setCredentials(credentialsProvider);
        } else if (credential instanceof DefaultCredential) {
            // use builder default
        } else {
            throw new InvalidCredentialException(ADAPTOR_NAME, "Password of Default credential required");
        }
        AWSBatch client = builder.withRegion(location).build();
        XenonProperties xp = new XenonProperties(VALID_PROPERTIES, properties);
        return new AWSBatchScheduler(getNewUniqueID(), ADAPTOR_NAME, location, credential, client, xp);
    }

    @Override
    public Class[] getSupportedCredentials() {
        return new Class[] {PasswordCredential.class, DefaultCredential.class};
    }

    @Override
    public boolean usesFileSystem() {
        return false;
    }
}
