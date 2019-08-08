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
package nl.esciencecenter.xenon.adaptors.filesystems.azure;

import java.net.URI;
import java.util.Map;

import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.InvalidPropertyException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.XenonPropertyDescription;
import nl.esciencecenter.xenon.XenonPropertyDescription.Type;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.filesystems.FileAdaptor;
import nl.esciencecenter.xenon.adaptors.filesystems.jclouds.JCloudsFileSytem;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;

/**
 * Created by atze on 29-6-17.
 */
public class AzureFileAdaptor extends FileAdaptor {

    /** The name of this adaptor */
    public static final String ADAPTOR_NAME = "azure";

    /** A description of this adaptor */
    private static final String ADAPTOR_DESCRIPTION = "The Azure adaptor uses Apache JClouds to talk to the Azure blobstore. To authenticate use PasswordCredential with access key id as username and secret access key as password";

    /** All our own properties start with this prefix. */
    public static final String PREFIX = FileAdaptor.ADAPTORS_PREFIX + ADAPTOR_NAME + ".";

    /** The buffer size to use when copying data. */
    public static final String BUFFER_SIZE = PREFIX + "bufferSize";

    /** The locations supported by this adaptor */
    private static final String[] ADAPTOR_LOCATIONS = new String[] { "http[s]://<account-name>.blob.core.windows.net/<container>[/workdir]" };

    /** List of properties supported by this Azure adaptor */
    private static final XenonPropertyDescription[] VALID_PROPERTIES = new XenonPropertyDescription[] {
            new XenonPropertyDescription(BUFFER_SIZE, Type.SIZE, "64K", "The buffer size to use when copying files (in bytes).") };

    public AzureFileAdaptor() {
        super("azure", ADAPTOR_DESCRIPTION, ADAPTOR_LOCATIONS, VALID_PROPERTIES);
    }

    @Override
    public FileSystem createFileSystem(String location, Credential credential, Map<String, String> properties) throws XenonException {

        // An Azure URI has the form:
        //
        // http[s]://<account-name>.blob.core.windows.net/<container>[/workdir]
        //
        // Note that the <container> may be empty, in which case the root container is used. The root container can also be explicitly addressed using:
        //
        // http[s]://<account-name>.blob.core.windows.net/$root[/workdir]
        //
        // Note that accessing the 'root' of a container does not seem to work, for example accessing:
        //
        // https://xenontest.blob.core.windows.net/testcontainer/
        //
        // results in an error, while accessing
        //
        // https://xenontest.blob.core.windows.net/testcontainer/test
        //
        // works fine.

        if (location == null || location.isEmpty()) {
            throw new InvalidLocationException(ADAPTOR_NAME, "Location may not be empty");
        }

        if (!(location.startsWith("http://") || location.startsWith("https://"))) {
            throw new InvalidLocationException(ADAPTOR_NAME, "Location must start with http[s]:// " + location);
        }

        if (credential == null) {
            throw new InvalidCredentialException(ADAPTOR_NAME, "Credential may not be null.");
        }

        if (!(credential instanceof PasswordCredential /* || credential instanceof DefaultCredential */)) {
            throw new InvalidCredentialException(ADAPTOR_NAME, "Credential type not supported");
        }

        XenonProperties xp = new XenonProperties(VALID_PROPERTIES, properties);

        long bufferSize = xp.getSizeProperty(BUFFER_SIZE);

        if (bufferSize <= 0 || bufferSize >= Integer.MAX_VALUE) {
            throw new InvalidPropertyException(ADAPTOR_NAME,
                    "Invalid value for " + BUFFER_SIZE + ": " + bufferSize + " (must be between 1 and " + Integer.MAX_VALUE + ")");
        }

        String server = null;
        String bucket = null;
        String bucketPath = null;
        Path path = null;

        URI uri;

        try {
            uri = new URI(location);
        } catch (Exception e) {
            throw new InvalidLocationException(ADAPTOR_NAME, "Failed to parse location: " + location, e);
        }

        // Reconstruct the server address
        server = uri.getScheme() + "://" + uri.getHost() + (uri.getPort() != -1 ? ":" + uri.getPort() : "");
        bucketPath = uri.getPath();

        if (bucketPath == null || bucketPath.isEmpty() || bucketPath.equals("/")) {
            // throw new InvalidLocationException(ADAPTOR_NAME, "Location does not contain bucket: " + location);
            bucketPath = "/";
        }

        if (bucketPath.startsWith("/")) {
            bucketPath = bucketPath.substring(1);
        }

        int split = bucketPath.indexOf('/');

        if (split < 0) {
            bucket = bucketPath;
            path = new Path('/', "/");
        } else {
            // Split the bucket and the working dir in path.
            bucket = bucketPath.substring(0, split);
            path = new Path('/', bucketPath.substring(split));
        }

        PasswordCredential pwUser = (PasswordCredential) credential;

        BlobStoreContext context = ContextBuilder.newBuilder("azureblob").endpoint(server).credentials(pwUser.getUsername(), new String(pwUser.getPassword()))
                .buildView(BlobStoreContext.class);

        return new JCloudsFileSytem(getNewUniqueID(), ADAPTOR_NAME, server, credential, path, context, bucket, (int) bufferSize, xp);
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
        return new Class[] { PasswordCredential.class };
    }
}
