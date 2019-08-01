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
package nl.esciencecenter.xenon.adaptors.filesystems.s3;

import java.net.URI;
import java.util.Map;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.InvalidPropertyException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.XenonPropertyDescription.Type;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.filesystems.FileAdaptor;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;

/**
 * Created by atze on 29-6-17.
 */
public class S3FileAdaptor extends FileAdaptor {

    /** The name of this adaptor */
    public static final String ADAPTOR_NAME = "s3";

    /** A description of this adaptor */
    private static final String ADAPTOR_DESCRIPTION = "AWS S3 blob store filesystem adaptor";

    /** All our own properties start with this prefix. */
    public static final String PREFIX = FileAdaptor.ADAPTORS_PREFIX + ADAPTOR_NAME + ".";

    /** The buffer size to use when copying data. */
    public static final String BUFFER_SIZE = PREFIX + "bufferSize";

    /** The locations supported by this adaptor */
    private static final String[] ADAPTOR_LOCATIONS = new String[] { "[http[s]://host[:port]]/bucketname[/workdir]" };

    public static final String REGION = PREFIX + "region";
    /** List of properties supported by this S3 adaptor */
    private static final XenonPropertyDescription[] VALID_PROPERTIES = new XenonPropertyDescription[] {
            new XenonPropertyDescription(BUFFER_SIZE, Type.SIZE, "64K", "The buffer size to use when copying files (in bytes)."),
            new XenonPropertyDescription(REGION, Type.STRING, "us-west-1", "The AWS region")
    };
    public static final String DEFAULT_ENDPOINT = "https://s3.amazonaws.com";

    public S3FileAdaptor() {
        super("s3", ADAPTOR_DESCRIPTION, ADAPTOR_LOCATIONS, VALID_PROPERTIES);
    }

    @Override
    public FileSystem createFileSystem(String location, Credential credential, Map<String, String> properties) throws XenonException {

        S3ClientBuilder builder = S3Client.builder();

        XenonProperties xp = new XenonProperties(VALID_PROPERTIES, properties);
        String region = xp.getStringProperty(REGION);
        if (!"".equals(region)) {
            builder = builder.region(Region.of(region));
        }

        if (credential == null || credential instanceof DefaultCredential) {
            // TODO remove profile provider, just here for keeping creds secret for temporary tests
            builder = builder.credentialsProvider(ProfileCredentialsProvider.create("xenon"));
            // default is using system props or env vars or profile config file
        } else  if (credential instanceof PasswordCredential) {
            PasswordCredential pwUser = (PasswordCredential) credential;
            AwsCredentials awsCreds = AwsBasicCredentials.create(pwUser.getUsername(), new String(pwUser.getPassword()));
            builder = builder.credentialsProvider(StaticCredentialsProvider.create(awsCreds));
        } else {
            throw new InvalidCredentialException(ADAPTOR_NAME, "Credential type not supported");
        }

        URI server = null;
        String bucket = null;
        String bucketPath = null;
        Path workDirectory = null;

        if (location == null || location.isEmpty()) {
            throw new InvalidLocationException(ADAPTOR_NAME, "Location may not be empty");
        }
        if (location.startsWith("http://") || location.startsWith("https://")) {
            URI uri;
            try {
                uri = new URI(location);
                server =  new URI(uri.getScheme() + "://" + uri.getHost() + (uri.getPort() != -1 ? ":" + uri.getPort() : ""));
                // Reconstruct the server address
                bucketPath = uri.getPath();
            } catch (Exception e) {
                throw new InvalidLocationException(ADAPTOR_NAME, "Failed to parse location: " + location, e);
            }

        } else {
            bucketPath = location;
        }

        if (bucketPath == null || bucketPath.isEmpty() || bucketPath.equals("/")) {
            throw new InvalidLocationException(ADAPTOR_NAME, "Location does not contain bucket: " + location);
        }

        if (bucketPath.startsWith("/")) {
            bucketPath = bucketPath.substring(1);
        }

        int split = bucketPath.indexOf('/');

        if (split < 0) {
            bucket = bucketPath;
            workDirectory = new Path('/', "/");
        } else {
            // Split the bucket and the working dir in path.
            bucket = bucketPath.substring(0, split);
            workDirectory = new Path('/', bucketPath.substring(split));
        }

        if (server != null) {
            builder = builder.endpointOverride(server);
            builder = builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }

        S3Client client = builder.build();

        long bufferSize = xp.getSizeProperty(BUFFER_SIZE);

        if (bufferSize <= 0 || bufferSize >= Integer.MAX_VALUE) {
            throw new InvalidPropertyException(ADAPTOR_NAME,
                    "Invalid value for " + BUFFER_SIZE + ": " + bufferSize + " (must be between 1 and " + Integer.MAX_VALUE + ")");
        }

        return new S3FileSystem(getNewUniqueID(), ADAPTOR_NAME, location, credential, workDirectory, client, bucket, (int) bufferSize, xp);
    }

    @Override
    public boolean supportsReadingPosixPermissions() {
        return false;
    }

    @Override
    public boolean supportsSettingPosixPermissions() {
        return false;
    }

    @Override
    public boolean canAppend() {
        return false;
    }

    @Override
    public boolean canReadSymboliclinks() {
        return false;
    }

    @Override
    public boolean canCreateSymboliclinks() {
        return false;
    }

    @Override
    public boolean needsSizeBeforehand() {
        return true;
    }

    @Override
    public boolean supportsRename() {
        return false;
    }

    @Override
    public boolean isConnectionless() {
        return true;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Class[] getSupportedCredentials() {
        // The S3 adaptor supports these credentials
        return new Class[] {DefaultCredential.class, PasswordCredential.class };
    }
}
