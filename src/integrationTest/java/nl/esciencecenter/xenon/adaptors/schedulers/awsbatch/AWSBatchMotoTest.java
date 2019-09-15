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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.SchedulerLocationConfig;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.credentials.PasswordCredential;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;

public class AWSBatchMotoTest extends AWSBatchSchedulerTestParent {

    @Rule
    public MotoServerRule moto = new MotoServerRule();

    @Override
    protected SchedulerLocationConfig setupLocationConfig() {
        return new AWSBatchLocationConfig(moto);
    }

    @Override
    public Scheduler setupScheduler(SchedulerLocationConfig config) throws XenonException {
        String location = moto.getEndpoint();
        Credential creds = new PasswordCredential("someaccesskey", "somesecretkey");
        Map<String, String> props = new HashMap<>();
        return Scheduler.create("awsbatch", location, creds, props);
    }

    @Override
    public void test_getJobStatus_finishedJob() throws XenonException {
        assumeFalse(description.isEmbedded());
        assumeTrue(description.supportsBatch());

        JobDescription jobdescription = new JobDescription();
        // moto_server ignores the job description and will always run 1+1+(10*1) sleep with some output to stdout
        String jobID = scheduler.submitBatchJob(jobdescription);
        scheduler.waitUntilDone(jobID, setupLocationConfig().getMaxWaintUntilDone());

        JobStatus status = scheduler.getJobStatus(jobID);

        assertNotNull(status);
        Assert.assertEquals(jobID, status.getJobIdentifier());
        assertTrue(status.isDone());
    }

    @Override
    public void test_sleep() throws XenonException {
        assumeTrue(description.supportsBatch());
        JobDescription jobdescription = new JobDescription();
        // moto_server ignores the job description and will always run 1+1+(10*1) sleep with some output to stdout
        String jobID = scheduler.submitBatchJob(jobdescription);

        JobStatus status = scheduler.waitUntilDone(jobID, setupLocationConfig().getMaxWaintUntilDone());

        assertTrue("Job is not done yet", status.isDone());
    }
}
