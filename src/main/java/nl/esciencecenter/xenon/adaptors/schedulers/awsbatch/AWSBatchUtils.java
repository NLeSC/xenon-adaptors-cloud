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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.batch.model.ArrayProperties;
import com.amazonaws.services.batch.model.ContainerDetail;
import com.amazonaws.services.batch.model.ContainerOverrides;
import com.amazonaws.services.batch.model.JobDefinition;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.JobQueueDetail;
import com.amazonaws.services.batch.model.JobTimeout;
import com.amazonaws.services.batch.model.KeyValuePair;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.OutputLogEvent;

import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.schedulers.JobStatusImplementation;
import nl.esciencecenter.xenon.adaptors.schedulers.QueueStatusImplementation;
import nl.esciencecenter.xenon.schedulers.InvalidJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.QueueStatus;

public class AWSBatchUtils {
    static final String QUEUE_SEPARATOR = "!";
    static final String JOBDEFINITION_SEPARATOR = ":";
    private static final String DEFAULT_JOBNAME = "xenon";

    static QueueStatus mapToQueueStatus(AWSBatchScheduler awsBatchScheduler, JobQueueDetail queue, JobDefinition definition) {
        Map<String, String> info = new HashMap<>();
        info.put("queue.arn", queue.getJobQueueArn());
        info.put("queue.name", queue.getJobQueueName());
        info.put("queue.priority", String.valueOf(queue.getPriority()));
        info.put("queue.state", queue.getState());
        info.put("queue.status", queue.getStatus());
        info.put("queue.statusReason", queue.getStatusReason());
        info.put("queue.computeEnvironmentOrder", queue.getComputeEnvironmentOrder().toString());
        info.put("definition.arn", definition.getJobDefinitionArn());
        info.put("definition.name", definition.getJobDefinitionName());
        info.put("definition.revision", String.valueOf(definition.getRevision()));
        info.put("definition.status", definition.getStatus());
        info.put("definition.timeout", String.valueOf(definition.getTimeout()));
        info.put("definition.nodeProperties", String.valueOf(definition.getNodeProperties()));
        info.put("definition.parameters", String.valueOf(definition.getParameters()));
        info.put("definition.retryStrategy", String.valueOf(definition.getRetryStrategy()));
        info.put("definition.type", definition.getType());

        String queueName = definition.getJobDefinitionName() + JOBDEFINITION_SEPARATOR + definition.getRevision() + QUEUE_SEPARATOR + queue.getJobQueueName();
        return new QueueStatusImplementation(awsBatchScheduler, queueName, null, info);
    }

    static JobStatus mapJobStatus(AWSBatchScheduler awsBatchScheduler, JobDetail jobResult) {
        com.amazonaws.services.batch.model.JobStatus awsJobStatus = com.amazonaws.services.batch.model.JobStatus.fromValue(jobResult.getStatus());
        boolean running = com.amazonaws.services.batch.model.JobStatus.RUNNING.equals(awsJobStatus);
        boolean done = com.amazonaws.services.batch.model.JobStatus.SUCCEEDED.equals(awsJobStatus) || com.amazonaws.services.batch.model.JobStatus.FAILED.equals(awsJobStatus);
        XenonException exception = null;
        if (com.amazonaws.services.batch.model.JobStatus.FAILED.equals(awsJobStatus)) {
            exception = new XenonException(awsBatchScheduler.getAdaptorName(), jobResult.getStatusReason());
        }
        Map<String, String> info = new HashMap<>();
        ContainerDetail container = jobResult.getContainer();
        Integer exitCode = null;
        if (container != null) {
            exitCode = container.getExitCode();
            if (exitCode == null && done) {
                if (com.amazonaws.services.batch.model.JobStatus.FAILED.equals(awsJobStatus)) {
                    exitCode = 1;
                } else {
                    exitCode = 0;
                }
            }
            info.put("image", container.getImage());
            info.put("logStreamName", container.getLogStreamName());
            info.put("containerInstanceArn", container.getContainerInstanceArn());
            info.put("taskArn", container.getTaskArn());
        }
        info.put("createdAt", String.valueOf(jobResult.getCreatedAt()));
        info.put("stoppedAt", String.valueOf(jobResult.getStartedAt()));
        info.put("status", jobResult.getStatus());
        info.put("statusReason", jobResult.getStatusReason());
        info.put("definition", jobResult.getJobDefinition());
        info.put("queue", jobResult.getJobQueue());
        // TODO add more fields from jobResult to info
        return new JobStatusImplementation(jobResult.getJobId(), jobResult.getJobName(), awsJobStatus.toString(), exitCode, exception, running, done, info);
    }

    private static String getRegionFromArn(String arn) {
        // ARN format = arn:aws:<vendor>:<region>:<namespace>:<relative-id>
        return arn.split(":")[3];
    }

    static boolean sleep(long pollDelay) {
        try {
            Thread.sleep(pollDelay);
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return true;
        }
    }

    static SubmitJobRequest mapToSubmitJobRequest(JobDescription description, String jobQueue, String jobDefinition) throws InvalidJobDescriptionException {
        ContainerOverrides containerOverride = new ContainerOverrides();
        List<String> command = new ArrayList<>();
        if (description.getTempSpace() != -1) {
            throw new InvalidJobDescriptionException("awsbatch", "AWS Batch can not guarantee free temporary space is available");
        }
        if (description.getExecutable() != null) {
            command.add(description.getExecutable());
            command.addAll(description.getArguments());
            containerOverride.setCommand(command);
        }
        if (description.getMaxMemory() > 0) {
            containerOverride.setMemory(description.getMaxMemory());
        }
        if (!description.getEnvironment().isEmpty()) {
            containerOverride.setEnvironment(mapEnvironment(description.getEnvironment()));
        }
        SubmitJobRequest submitJobRequest = new SubmitJobRequest()
            .withJobDefinition(jobDefinition)
            .withJobQueue(jobQueue)
            .withContainerOverrides(containerOverride)
            ;

        if (description.getTasksPerNode() > 1) {
            throw new InvalidJobDescriptionException("awsbatch", "AWS Batch can not run multiple tasks per node");
        }
        if (description.getTasks() > 1) {
            if (description.isStartPerTask()) {
                throw new InvalidJobDescriptionException("awsbatch", "AWS Batch can not run multiple tasks per node");
            } else {
                // Job executable must used AWS_BATCH_JOB_ARRAY_INDEX env var to do something different on each node
                ArrayProperties arrayProperties = new ArrayProperties().withSize(description.getTasks());
                submitJobRequest.withArrayProperties(arrayProperties);
            }
        }
        if (description.getCoresPerTask() != 1 ) {
            containerOverride.setVcpus(description.getCoresPerTask());
        }

        if (description.getName() != null) {
            submitJobRequest.setJobName(description.getName());
        } else {
            // name is required
            submitJobRequest.setJobName(DEFAULT_JOBNAME);
        }
        if (description.getMaxRuntime() != -1) {
            submitJobRequest.setTimeout(new JobTimeout().withAttemptDurationSeconds(description.getMaxRuntime() * 60));
        }
        // The following AWS Batch job request fields are not supported:
        // - parameters
        // - nr of job attempts
        // - job depends on some jobId
        // - sequential and n to n dependencies for array jobs
        // - number of GPUs
        // - container overrides besides command, environment, memory and vcpus
        // - nodeOverrides
        // They could be parsed from
        // List<String> schedulerArguments = description.getSchedulerArguments();
        return submitJobRequest;
    }

    private static Collection<KeyValuePair> mapEnvironment(Map<String, String> environment) {
        return environment.entrySet().stream().map(e -> new KeyValuePair().withName(e.getKey()).withValue(e.getValue())).collect(Collectors.toList());
    }

    /**
     * Retrieves the log of a done AWS Batch job from AWS Cloudwatch
     *
     * @param status Status of an AWS Batch job, the job should be done (status.isDone() == true)
     * @param accessKey Access key used for AWS Cloudwatch logs client
     * @param secretKey Secret key used for AWS Cloudwatch logs client
     * @return Log of job
     */
    public static String getLog(JobStatus status, String accessKey, String secretKey) {
        AWSStaticCredentialsProvider credProv = new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
        // Use same region as the where the task was run
        String region = getRegionFromArn(status.getSchedulerSpecificInformation().get("taskArn"));
        AWSLogs lclient = AWSLogsClient.builder().withCredentials(credProv).withRegion(region).build();
        String logStreamName = status.getSchedulerSpecificInformation().get("logStreamName");
        List<OutputLogEvent> events = lclient.getLogEvents(new GetLogEventsRequest().withLogGroupName("/aws/batch/job").withLogStreamName(logStreamName)).getEvents();
        String log = events.stream().map(OutputLogEvent::getMessage).collect(Collectors.joining(System.lineSeparator()));
        lclient.shutdown();
        return log;
    }
}
