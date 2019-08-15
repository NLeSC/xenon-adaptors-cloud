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

import static nl.esciencecenter.xenon.adaptors.schedulers.awsbatch.AWSBatchUtils.mapToSubmitJobRequest;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.batch.model.ArrayJobDependency;
import com.amazonaws.services.batch.model.ContainerOverrides;
import com.amazonaws.services.batch.model.JobDependency;
import com.amazonaws.services.batch.model.KeyValuePair;
import com.amazonaws.services.batch.model.ResourceRequirement;
import com.amazonaws.services.batch.model.ResourceType;
import com.amazonaws.services.batch.model.RetryStrategy;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import nl.esciencecenter.xenon.schedulers.InvalidJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.JobDescription;

public class AWSBatchUtilsTest {
    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Test
    public void mapToSubmitJobRequest_maxMemory() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setMaxMemory(1024);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        int actual = request.getContainerOverrides().getMemory();
        int expected = 1024;
        assertEquals(expected, actual);
    }

    @Test
    public void mapToSubmitJobRequest_environment() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        Map<String, String> environment = Map.of("SOME_NAME", "42", "OTHER_NAME", "other_val");
        description.setEnvironment(environment);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        List<KeyValuePair> actual = request.getContainerOverrides().getEnvironment();
        List<KeyValuePair> expected = List.of(
            new KeyValuePair().withName("SOME_NAME").withValue("42"),
            new KeyValuePair().withName("OTHER_NAME").withValue("other_val")
        );
        assertEquals(expected, actual);
    }

    @Test
    public void mapToSubmitJobRequest_manyRasksPerNode() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("AWS Batch can not run multiple tasks per node");

        JobDescription description = new JobDescription();
        description.setTasksPerNode(42);

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_startPerRask() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("AWS Batch can not run multiple tasks per node");

        JobDescription description = new JobDescription();
        description.setTasks(42);
        description.setStartPerTask();

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_multiNode() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setTasksPerNode(1);
        description.setTasks(42);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        int actual = request.getArrayProperties().getSize();
        int expected = 42;
        assertEquals(expected, actual);
    }

    @Test
    public void mapToSubmitJobRequest_multiCore() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setTasks(1);
        description.setCoresPerTask(42);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        int actual = request.getContainerOverrides().getVcpus();
        int expected = 42;
        assertEquals(expected, actual);
    }

    @Test
    public void mapToSubmitJobRequest_multiCoreAndMultiNode() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setTasksPerNode(1);
        description.setTasks(2);
        description.setCoresPerTask(4);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        assertEquals(4, request.getContainerOverrides().getVcpus().intValue());
        assertEquals(2, request.getArrayProperties().getSize().intValue());
    }

    @Test
    public void mapToSubmitJobRequest_customName() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setName("mynameisawesome");

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        assertEquals("mynameisawesome", request.getJobName());
    }

    @Test
    public void mapToSubmitJobRequest_defaultName() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        assertEquals("xenon", request.getJobName());
    }

    @Test
    public void mapToSubmitJobRequest_maxRuntime() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setMaxRuntime(5);

        SubmitJobRequest request = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        assertEquals(5 * 60, request.getTimeout().getAttemptDurationSeconds().intValue());
    }

    @Test
    public void mapToSubmitJobRequest_tempspace() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("AWS Batch can not guarantee free temporary space is available");

        JobDescription description = new JobDescription();
        description.setTempSpace(42);

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_argumentswithoutexecutable() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("AWS Batch submission can not have arguments without an executable");

        JobDescription description = new JobDescription();
        description.setArguments("arg1");

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_tooManySchedulerArguments() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("Only a single scheduler argument as a JSON string in AWS Batch SubmitJob request format is accepted");

        JobDescription description = new JobDescription();
        description.setSchedulerArguments("first", "second");

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_schedulerArgumentMalformedJSON() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("Unable to parse scheduler argument");

        JobDescription description = new JobDescription();
        description.setSchedulerArguments("foo:bar");

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_schedulerArgumentWithParameters() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        description.setSchedulerArguments("{\"parameters\": {\"inputfile\": \"avengers.ts\", \"outputfile\": \"avengers.mp4\"}}");

        SubmitJobRequest actual = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        Map<String, String> expected = Map.of("inputfile", "avengers.ts", "outputfile", "avengers.mp4");
        assertEquals(expected, actual.getParameters());
    }

    @Test
    public void mapToSubmitJobRequest_schedulerArgumentContainerOverridesAndMaxMemory() throws InvalidJobDescriptionException {
        exceptionRule.expect(InvalidJobDescriptionException.class);
        exceptionRule.expectMessage("awsbatch");
        exceptionRule.expectMessage("Scheduler argument contains a container overrides which conflicts with executable");

        JobDescription description = new JobDescription();
        String schedulerArgumentWithContainerOverrides = "{\n" +
            "    \"containerOverrides\": { \n" +
            "        \"memory\": 512\n" +
            "    }\n" +
            "}";
        description.setSchedulerArguments(schedulerArgumentWithContainerOverrides);
        description.setMaxMemory(256);

        mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");
    }

    @Test
    public void mapToSubmitJobRequest_schedulerArgumentKitchenSink() throws InvalidJobDescriptionException {
        JobDescription description = new JobDescription();
        String schedulerArgumentWithContainerOverrides = "{\n" +
            "    \"parameters\": {\n" +
            "        \"inputfile\": \"avengers.ts\",\n" +
            "        \"outputfile\": \"avengers.mp4\"\n" +
            "    },\n" +
            "    \"retryStrategy\": {\n" +
            "        \"attempts\": 2\n" +
            "    },\n" +
            "    \"containerOverrides\": { \n" +
            "        \"memory\": 512,\n" +
            "        \"resourceRequirements\": [\n" +
            "            {\n" +
            "                \"value\": \"1\",\n" +
            "                \"type\": \"GPU\"\n" +
            "            }\n" +
            "        ]\n" +
            "    },\n" +
            "    \"dependsOn\": [{\n" +
            "        \"type\": \"SEQUENTIAL\",\n" +
            "        \"jobId\": \"jobdef2/jobqueue1/d6cb9d3d-6262-425e-844a-8d45341c36ba\"\n" +
            "    }]\n" +
            "}";
        description.setSchedulerArguments(schedulerArgumentWithContainerOverrides);

        SubmitJobRequest actual = mapToSubmitJobRequest(description, "jobqueue1", "jobdefinition1");

        SubmitJobRequest expected = new SubmitJobRequest()
            .withJobName("xenon")
            .withJobQueue("jobqueue1")
            .withJobDefinition("jobdefinition1")
            .withParameters(Map.of("inputfile", "avengers.ts", "outputfile", "avengers.mp4"))
            .withRetryStrategy(new RetryStrategy().withAttempts(2))
            .withContainerOverrides(new ContainerOverrides()
                .withMemory(512)
                .withResourceRequirements(List.of(new ResourceRequirement().withType(ResourceType.GPU).withValue("1")))
            )
            .withDependsOn(new JobDependency().withType(ArrayJobDependency.SEQUENTIAL).withJobId("jobdef2/jobqueue1/d6cb9d3d-6262-425e-844a-8d45341c36ba"))
            ;

        assertEquals(expected, actual);
    }
}
