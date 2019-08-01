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

import nl.esciencecenter.xenon.credentials.DefaultCredential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.filesystems.Path;
import nl.esciencecenter.xenon.filesystems.PathAttributes;
import org.junit.Test;

import nl.esciencecenter.xenon.InvalidCredentialException;
import nl.esciencecenter.xenon.InvalidLocationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.credentials.CredentialMap;
import nl.esciencecenter.xenon.credentials.PasswordCredential;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class S3FileSystemSimpleTests {

    @Test(expected = InvalidCredentialException.class)
    public void test_invalid_credential_type() throws XenonException {
        new S3FileAdaptor().createFileSystem("localhost", new CredentialMap(), null);
    }

    @Test(expected = InvalidCredentialException.class)
    public void test_credential_null() throws XenonException {
        new S3FileAdaptor().createFileSystem("localhost", null, null);
    }

    @Test(expected = InvalidLocationException.class)
    public void test_location_null() throws XenonException {
        new S3FileAdaptor().createFileSystem(null, new PasswordCredential("aap", "noot".toCharArray()), null);
    }

    @Test(expected = InvalidLocationException.class)
    public void test_location_empty() throws XenonException {
        new S3FileAdaptor().createFileSystem("", new PasswordCredential("aap", "noot".toCharArray()), null);
    }

    @Test
    public void test_aws() throws XenonException, IOException {
        Map<String, String> props = new HashMap<>();
        props.put("xenon.adaptors.filesystems.s3.region", "eu-central-1");
        FileSystem fs = FileSystem.create("s3", "/xenontest", new DefaultCredential(), props);

        Path path = new Path("/params.json");
//        PathAttributes attribs = fs.getAttributes(path);
//        assertEquals(attribs.getSize(), 3504);
//        assertTrue(fs.exists(path));
//        assertFalse(fs.exists(new Path("idontexist")));
//        fs.delete(new Path("idontexist"), false);
//        fs.createDirectory(new Path("foo2"));
//        fs.createDirectories(new Path("foo3/foo4"));
        fs.delete(new Path("foo2"), false);
        fs.list(new Path("/"), true).forEach(pa -> System.out.println(pa.getPath().toString()));

//        assertEquals("bla", new String(fs.readFromFile(path).readAllBytes()));

            // TODO fix write
//        OutputStream stream = fs.writeToFile(new Path("somedir/somefile"), 9);
//        stream.write("something".getBytes());
//        stream.flush();
//        stream.close();
//        assertTrue(fs.exists(new Path("somedir/somefile")));
    }

    @Test
    public void test_aws_override_endpoint() throws XenonException {
        FileSystem fs = FileSystem.create("s3", "https://s3.eu-central-1.amazonaws.com/xenontest", new DefaultCredential());

        PathAttributes attribs = fs.getAttributes(new Path("/params.json"));
        assertEquals(attribs.getSize(), 3504);
    }

    @Test
    public void test_minio() throws XenonException {
        PasswordCredential cred = new PasswordCredential("xenon", "javagat01");
        FileSystem fs = FileSystem.create("s3", "http://localhost:9000/filesystem-test-fixture", cred);

        PathAttributes attribs = fs.getAttributes(new Path("/links/file0"));
        assertEquals(attribs.getSize(), 12);
    }
}
