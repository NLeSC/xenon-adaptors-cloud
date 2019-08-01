package nl.esciencecenter.xenon.adaptors.filesystems.s3;

import nl.esciencecenter.xenon.UnsupportedOperationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.filesystems.PathAttributesImplementation;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.*;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.*;
import java.util.Iterator;
import java.util.Set;

public class S3FileSystem extends FileSystem {
    private final S3Client client;
    private final String bucket;
    private final String adaptorName;

    public S3FileSystem(String uniqueID, String adaptor, String location, Credential credential, Path workDirectory, S3Client client, String bucket, int bufferSize, XenonProperties properties) {
        super(uniqueID, adaptor, location, credential, workDirectory, bufferSize, properties);
        this.client = client;
        this.bucket = bucket;
        this.adaptorName = adaptor;
    }

    @Override
    public boolean isOpen() throws XenonException {
        return true;
    }

    @Override
    public void rename(Path source, Path target) throws XenonException {
        throw new UnsupportedOperationException(adaptorName, "This adaptor does not support renaming.");
    }

    @Override
    public void createDirectory(Path dir) throws XenonException {
        String key = toKey(dir);
        if (!key.endsWith("/")) {
            key += "/";
        }
        PutObjectRequest obj = PutObjectRequest.builder().bucket(bucket).key(key).build();
        RequestBody blob = RequestBody.empty();
        client.putObject(obj, blob);
    }

    @Override
    public void createFile(Path file) throws XenonException {
        String key = toKey(file);

        PutObjectRequest obj = PutObjectRequest.builder().bucket(bucket).key(key).build();
        RequestBody blob = RequestBody.empty();
        client.putObject(obj, blob);
    }

    @Override
    public void createSymbolicLink(Path link, Path target) throws XenonException {
        throw new AttributeNotSupportedException(adaptorName, "Symbolic link  not supported by " + adaptorName);
    }

    @Override
    public boolean exists(Path path) throws XenonException {
        String key = toKey(path);
        if (key.equals("")) {
            return true;
        }
        HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(key).build();

        try {
            client.headObject(req);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        }
    }

    @Override
    public InputStream readFromFile(Path file) throws XenonException {
        String key = toKey(file);

        return client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
    }

    @Override
    public OutputStream writeToFile(Path path, long size) throws XenonException {
        String key = toKey(path);
        OutputStream out;

        final PipedInputStream read = new PipedInputStream();
        RequestBody blob = RequestBody.fromInputStream(read, size);
        PutObjectRequest req = PutObjectRequest.builder().bucket(bucket).key(key).build();

        try {
            out = new PipedOutputStream(read);
        } catch (IOException e) {
            throw new XenonException(adaptorName, "IO error when trying to write: " + e.getMessage(), e);
        }
        new Thread(() -> client.putObject(req, blob)).start();

        return out;
    }

    @Override
    public OutputStream writeToFile(Path file) throws XenonException {
        throw new UnsupportedOperationException(adaptorName, "WriteToFile without predefined size not supported");
    }

    @Override
    public OutputStream appendToFile(Path file) throws XenonException {
        throw new UnsupportedOperationException(adaptorName, "Append not supported");
    }

    @Override
    public PathAttributes getAttributes(Path path) throws XenonException {
        String key = toKey(path);
        if (key.equals("")) {
            PathAttributesImplementation attribs = new PathAttributesImplementation();
            attribs.setPath(new Path("/"));
            attribs.setDirectory(true);
            return attribs;
        }
        HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(key).build();

        HeadObjectResponse res = client.headObject(req);

        PathAttributesImplementation attribs = new PathAttributesImplementation();
        attribs.setPath(new Path("/" + key));
        attribs.setSize(res.contentLength());
        attribs.setLastModifiedTime(res.lastModified().toEpochMilli()); // TODO or seconds?
        attribs.setDirectory(true);
        attribs.setRegular(true);
        attribs.setReadable(true);
        attribs.setWritable(true);
        return attribs;
    }

    private String toKey(Path path) {
        return toAbsolutePath(path).toRelativePath().toString();
    }

    @Override
    public Path readSymbolicLink(Path link) throws XenonException {
        throw new AttributeNotSupportedException(adaptorName, "Symbolic link  not supported by " + adaptorName);
    }

    @Override
    public void setPosixFilePermissions(Path path, Set<PosixFilePermission> permissions) throws XenonException {
        throw new UnsupportedOperationException(getAdaptorName(), "POSIX permissions not supported");
    }

    @Override
    protected void deleteFile(Path file) throws XenonException {
        String key = toKey(file);

        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            throw new NoSuchPathException(adaptorName, "Key does not exist: " + key, e);
        }
    }

    @Override
    protected void deleteDirectory(Path path) throws XenonException {
        String key = toKey(path);
        if (!key.endsWith("/")) {
            key += "/";
        }
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (NoSuchKeyException e) {
            throw new NoSuchPathException(adaptorName, "Key does not exist: " + key, e);
        }
    }

    @Override
    protected Iterable<PathAttributes> listDirectory(Path dir) throws XenonException {
        String prefix = toKey(dir);

        ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder().bucket(bucket);
        if (prefix.equals("")) {
            builder = builder.prefix(prefix);
        }
        ListObjectsV2Iterable pager = client.listObjectsV2Paginator(builder.build());
        Iterator<PathAttributes> iterator = pager.contents().stream().map(obj -> {
            PathAttributesImplementation pa = new PathAttributesImplementation();
            pa.setPath(new Path(obj.key()));
            pa.setRegular(true);
            pa.setSize(obj.size());
            pa.setLastModifiedTime(obj.lastModified().toEpochMilli());
            pa.setDirectory(obj.key().endsWith("/"));
            return (PathAttributes) pa;
        }).iterator();
        return () -> iterator;
    }

    @Override
    public void close() throws XenonException {
        client.close();
        super.close();
    }
}
