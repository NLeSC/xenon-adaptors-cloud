/*
 * Copyright 2018 Netherlands eScience Center
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.esciencecenter.xenon.adaptors.schedulers.awsbatch;

import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
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
    private static final String ADAPTOR_DESCRIPTION = "The AWS Batch adaptor can submit jobs to AWS Batch service. " +
        "To authenticate use PasswordCredential with access key id as username and secret access key as password. " +
        "Adaptor expects job queues and job definitions to have been created before use. " +
        "The scheduler queues are combinations of AWS Batch job definitions and AWS Batch job queues. " +
        "Logs of jobs are available in the AWS CloudWatch logs service and can be optionally fetched using the AWSBatchUtils.getLog method." +
        "AWS Batch submit fields that can not be mapped to a Xenon JobDescription field can be passed as a JSON string in AWS Batch SubmitJob request format as scheduler argument in the job description";

    /** All our own properties start with this prefix. */
    public static final String PREFIX = SchedulerAdaptor.ADAPTORS_PREFIX + ADAPTOR_NAME + ".";

    /** Polling delay for jobs started by this adaptor. */
    public static final String POLL_DELAY_PROPERTY = PREFIX + "poll.delay";

    /** The locations supported by this adaptor */
    private static final String[] ADAPTOR_LOCATIONS = new String[] { "region", "http://hostname:port"};

    /** List of properties supported by this AWS Batch adaptor */
    private static final XenonPropertyDescription[] VALID_PROPERTIES = new XenonPropertyDescription[] {
        new XenonPropertyDescription(POLL_DELAY_PROPERTY, XenonPropertyDescription.Type.LONG, "5000", "Number of milliseconds between polling the status of a job.")
    };

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
        if (location.matches("^http://.+:[0-9]+$")) {
            builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(location, "us-east-1"));
        } else {
            builder.setRegion(location);
        }
        AWSBatch client = builder.build();
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
