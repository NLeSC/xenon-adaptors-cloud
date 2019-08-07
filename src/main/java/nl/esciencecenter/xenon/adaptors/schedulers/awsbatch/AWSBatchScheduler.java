package nl.esciencecenter.xenon.adaptors.schedulers.awsbatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.ContainerOverrides;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsRequest;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsResult;
import com.amazonaws.services.batch.model.DescribeJobQueuesRequest;
import com.amazonaws.services.batch.model.DescribeJobQueuesResult;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.DescribeJobsResult;
import com.amazonaws.services.batch.model.JobDefinition;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.JobQueueDetail;
import com.amazonaws.services.batch.model.JobTimeout;
import com.amazonaws.services.batch.model.KeyValuePair;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.amazonaws.services.batch.model.TerminateJobRequest;

import nl.esciencecenter.xenon.UnsupportedOperationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.schedulers.JobStatusImplementation;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.schedulers.IncompleteJobDescriptionException;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.NoSuchJobException;
import nl.esciencecenter.xenon.schedulers.QueueStatus;
import nl.esciencecenter.xenon.schedulers.Scheduler;
import nl.esciencecenter.xenon.schedulers.Streams;

public class AWSBatchScheduler extends Scheduler {
    private final AWSBatch client;
    private boolean isShutdown = false;

    public AWSBatchScheduler(String uniqueID, String adaptorName, String location, Credential credential, AWSBatch client, XenonProperties properties) {
        super(uniqueID, adaptorName, location, credential, properties);
        this.client = client;
    }

    @Override
    public String[] getQueueNames() throws XenonException {
        DescribeJobDefinitionsResult jobDefinitions = client.describeJobDefinitions(new DescribeJobDefinitionsRequest());
        DescribeJobQueuesResult jobQueues = client.describeJobQueues(new DescribeJobQueuesRequest());
        List<String> queueNames = new ArrayList<String>();
        for (JobQueueDetail queue : jobQueues.getJobQueues()) {
            for (JobDefinition jobDefinition : jobDefinitions.getJobDefinitions()) {
                String queueName = jobDefinition.getJobDefinitionName() + "!" + queue.getJobQueueName();
                queueNames.add(queueName);
            }
        }

        return queueNames.toArray(String[]::new);
    }

    @Override
    public void close() throws XenonException {
        client.shutdown();
        isShutdown = true;
    }

    @Override
    public boolean isOpen() throws XenonException {
        return !isShutdown;
    }

    @Override
    public String getDefaultQueueName() throws XenonException {
        return getQueueNames()[0];
    }

    @Override
    public int getDefaultRuntime() throws XenonException {
        return 0;
    }

    @Override
    public String[] getJobs(String... queueNames) throws XenonException {
        return new String[0];
    }

    @Override
    public QueueStatus getQueueStatus(String queueName) throws XenonException {
        return null;
    }

    @Override
    public QueueStatus[] getQueueStatuses(String... queueNames) throws XenonException {
        return new QueueStatus[0];
    }

    @Override
    public String submitBatchJob(JobDescription description) throws XenonException {
        if (description.getQueueName() == null) {
            throw new IncompleteJobDescriptionException(getAdaptorName(), "AWS Batch must have queue name");
        }
        String[] queueNameParts = description.getQueueName().split("!");
        String jobDefinition = queueNameParts[0];
        String jobQueue = queueNameParts[1];
        List<String> command = new ArrayList<>();
        command.add(description.getExecutable());
        command.addAll(description.getArguments());
        ContainerOverrides containerOverride = new ContainerOverrides()
            .withCommand(command)
            .withMemory(description.getMaxMemory())
            .withVcpus(description.getTasks()) // TODO correct?
            .withEnvironment(mapEnvironment(description.getEnvironment()));
        SubmitJobRequest submitJobRequest = new SubmitJobRequest()
            .withJobName(description.getName())
            .withJobDefinition(jobDefinition)
            .withJobQueue(jobQueue)
            .withTimeout(new JobTimeout().withAttemptDurationSeconds(description.getMaxRuntime() * 60)) // TODO make optional
            .withContainerOverrides(containerOverride);

        SubmitJobResult result = client.submitJob(submitJobRequest);
        return result.getJobId();
    }

    private Collection<KeyValuePair> mapEnvironment(Map<String, String> environment) {
        return environment.entrySet().stream().map(e -> new KeyValuePair().withName(e.getKey()).withValue(e.getValue())).collect(Collectors.toList());
    }

    @Override
    public Streams submitInteractiveJob(JobDescription description) throws XenonException {
        throw new UnsupportedOperationException(getAdaptorName(), "AWS Batch does not support submitInteractiveJob");
    }

    @Override
    public JobStatus getJobStatus(String jobIdentifier) throws XenonException {
        DescribeJobsRequest request = new DescribeJobsRequest().withJobs(jobIdentifier);
        DescribeJobsResult result = client.describeJobs(request);
        if (result.getJobs().isEmpty()) {
            throw new NoSuchJobException(getAdaptorName(), jobIdentifier + " not found");
        }
        JobDetail jobResult = result.getJobs().get(0);
        return mapJobStatus(jobResult);
    }

    private JobStatus mapJobStatus(JobDetail jobResult) {
        int exitCode = 0; // TODO Xenon <> AWS mismatch, must document
        com.amazonaws.services.batch.model.JobStatus awsJobStatus = com.amazonaws.services.batch.model.JobStatus.fromValue(jobResult.getStatus());
        boolean running = com.amazonaws.services.batch.model.JobStatus.RUNNING.equals(awsJobStatus);
        boolean done = com.amazonaws.services.batch.model.JobStatus.SUCCEEDED.equals(awsJobStatus) || com.amazonaws.services.batch.model.JobStatus.FAILED.equals(awsJobStatus);
        XenonException exception = null;
        if (com.amazonaws.services.batch.model.JobStatus.FAILED.equals(awsJobStatus)) {
            exitCode = 1;
            exception = new XenonException(getAdaptorName(), jobResult.getStatusReason());
        }
        Map<String, String> info = new HashMap<>();
        info.put("createdAt", jobResult.getCreatedAt().toString());
        info.put("stoppedAt", jobResult.getStartedAt().toString());
        info.put("status", jobResult.getStatus());
        info.put("statusReason", jobResult.getStatusReason());
        info.put("definition", jobResult.getJobDefinition());
        info.put("name", jobResult.getJobName());
        info.put("queue", jobResult.getJobQueue());
        info.put("image", jobResult.getContainer().getImage());
        // TODO add more fields from jobResult to info
        return new JobStatusImplementation(jobResult.getJobId(), jobResult.getJobName(), awsJobStatus.toString(), exitCode, exception, running, done, info);
    }

    @Override
    public JobStatus cancelJob(String jobIdentifier) throws XenonException {
        client.terminateJob(new TerminateJobRequest().withJobId(jobIdentifier).withReason("Cancelled by Xenon cancelJob call"));
        return getJobStatus(jobIdentifier);
    }

    @Override
    public JobStatus waitUntilDone(String jobIdentifier, long timeout) throws XenonException {
        return null;
    }

    @Override
    public JobStatus waitUntilRunning(String jobIdentifier, long timeout) throws XenonException {
        return null;
    }

    @Override
    public FileSystem getFileSystem() throws XenonException {
        throw new UnsupportedOperationException(getAdaptorName(), "AWSBatch has no filesystem");
    }
}
