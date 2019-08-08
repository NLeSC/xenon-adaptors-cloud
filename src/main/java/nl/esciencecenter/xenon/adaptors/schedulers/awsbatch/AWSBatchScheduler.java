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

import static nl.esciencecenter.xenon.adaptors.schedulers.awsbatch.AWSBatchAdaptor.ADAPTOR_NAME;
import static nl.esciencecenter.xenon.adaptors.schedulers.awsbatch.AWSBatchAdaptor.POLL_DELAY_PROPERTY;
import static nl.esciencecenter.xenon.adaptors.schedulers.awsbatch.AWSBatchUtils.JOBDEFINITION_SEPARATOR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsRequest;
import com.amazonaws.services.batch.model.DescribeJobDefinitionsResult;
import com.amazonaws.services.batch.model.DescribeJobQueuesRequest;
import com.amazonaws.services.batch.model.DescribeJobQueuesResult;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.DescribeJobsResult;
import com.amazonaws.services.batch.model.JobDefinition;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.JobQueueDetail;
import com.amazonaws.services.batch.model.JobSummary;
import com.amazonaws.services.batch.model.ListJobsRequest;
import com.amazonaws.services.batch.model.ListJobsResult;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.amazonaws.services.batch.model.TerminateJobRequest;

import nl.esciencecenter.xenon.UnsupportedOperationException;
import nl.esciencecenter.xenon.XenonException;
import nl.esciencecenter.xenon.adaptors.XenonProperties;
import nl.esciencecenter.xenon.adaptors.schedulers.Deadline;
import nl.esciencecenter.xenon.credentials.Credential;
import nl.esciencecenter.xenon.filesystems.FileSystem;
import nl.esciencecenter.xenon.schedulers.JobDescription;
import nl.esciencecenter.xenon.schedulers.JobStatus;
import nl.esciencecenter.xenon.schedulers.NoSuchJobException;
import nl.esciencecenter.xenon.schedulers.NoSuchQueueException;
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
        List<String> queueNames = new ArrayList<>();
        for (JobQueueDetail queue : jobQueues.getJobQueues()) {
            for (JobDefinition jobDefinition : jobDefinitions.getJobDefinitions()) {
                String queueName = jobDefinition.getJobDefinitionName() + JOBDEFINITION_SEPARATOR + jobDefinition.getRevision() + AWSBatchUtils.QUEUE_SEPARATOR + queue.getJobQueueName();
                queueNames.add(queueName);
            }
        }

        return queueNames.toArray(String[]::new);
    }

    @Override
    public void close() {
        client.shutdown();
        isShutdown = true;
    }

    @Override
    public boolean isOpen() {
        return !isShutdown;
    }

    @Override
    public String getDefaultQueueName() throws XenonException {
        return getQueueNames()[0];
    }

    @Override
    public int getDefaultRuntime() {
        // Each job definition can have a timeout, so there is no default
        return -1;
    }

    @Override
    public String[] getJobs(String... queueNames) throws XenonException {
        List<String> jobIdentifiers = new ArrayList<>();
        String[] allQueueNames = getQueueNames();
        if (queueNames == null || queueNames.length == 0) {
            // use all queues when no selection is given
            queueNames = allQueueNames;
        }
        // check for invalid queues
        Set<String> invalidQueues = new HashSet<>(Set.of(queueNames));
        invalidQueues.removeAll(Set.of(allQueueNames));
        if (!invalidQueues.isEmpty()) {
            throw new NoSuchQueueException(ADAPTOR_NAME, "Invalid queues given: " + Arrays.toString(queueNames));
        }
        Set<com.amazonaws.services.batch.model.JobStatus> statusFilters = Set.of(
            com.amazonaws.services.batch.model.JobStatus.SUBMITTED,
            com.amazonaws.services.batch.model.JobStatus.PENDING,
            com.amazonaws.services.batch.model.JobStatus.RUNNABLE,
            com.amazonaws.services.batch.model.JobStatus.STARTING,
            com.amazonaws.services.batch.model.JobStatus.RUNNING,
            com.amazonaws.services.batch.model.JobStatus.SUCCEEDED,
            com.amazonaws.services.batch.model.JobStatus.FAILED
        );
        for (String queueName: queueNames) {
            for (com.amazonaws.services.batch.model.JobStatus statusFilter : statusFilters) {
                ListJobsResult result = client.listJobs(
                    new ListJobsRequest().withJobStatus(statusFilter).withJobQueue(queueName)
                );
                List<String> qJobs = result.getJobSummaryList().stream()
                    .map(JobSummary::getJobId).collect(Collectors.toList());
                jobIdentifiers.addAll(qJobs);
            }
        }
        return jobIdentifiers.toArray(String[]::new);
    }

    @Override
    public QueueStatus getQueueStatus(String queueName) throws XenonException {
        if (queueName == null) {
            throw new IllegalArgumentException("Queue name can not be null");
        }
        try {
            // Strip job definition from queue name
            String[] queueParts = queueName.split(AWSBatchUtils.QUEUE_SEPARATOR);
            String jobDefinitionName = queueParts[0].split(JOBDEFINITION_SEPARATOR)[0];
            String jobQueueName = queueParts[1];
            JobQueueDetail jobQueueDetail = client.describeJobQueues(new DescribeJobQueuesRequest().withJobQueues(jobQueueName)).getJobQueues().get(0);
            JobDefinition jobDefinition = client.describeJobDefinitions(new DescribeJobDefinitionsRequest().withJobDefinitions(jobDefinitionName)).getJobDefinitions().get(0);
            return AWSBatchUtils.mapToQueueStatus(this, jobQueueDetail, jobDefinition);
        } catch (IndexOutOfBoundsException e ) {
            throw new NoSuchQueueException(ADAPTOR_NAME, "Queue `" + queueName + "` does not exist");
        }
    }

    @Override
    public QueueStatus[] getQueueStatuses(String... queueNames) throws XenonException {
        if (queueNames == null) {
            throw new IllegalArgumentException("Queue name can not be null");
        }
        Supplier<Stream<String[]>> qstream = () -> List.of(queueNames).stream().map(q -> q.split(AWSBatchUtils.QUEUE_SEPARATOR));
        Set<String> defs2fetch = qstream.get().map(q -> q[0].split(JOBDEFINITION_SEPARATOR)[0]).collect(Collectors.toSet());
        Set<String> queues2fetch = qstream.get().map(q -> q[1]).collect(Collectors.toSet());

        Map<String, JobQueueDetail> queues = client.describeJobQueues(new DescribeJobQueuesRequest().withJobQueues(queues2fetch)).getJobQueues().stream().collect(
            Collectors.toMap(JobQueueDetail::getJobQueueName, q -> q)
        );
        Map<String, JobDefinition> definitions = client.describeJobDefinitions(new DescribeJobDefinitionsRequest().withJobDefinitions(defs2fetch)).getJobDefinitions().stream().collect(
            Collectors.toMap(d -> d.getJobDefinitionName() + JOBDEFINITION_SEPARATOR + d.getRevision(), d -> d)
        );
        return qstream.get().map(qp -> AWSBatchUtils.mapToQueueStatus(this, queues.get(qp[1]), definitions.get(qp[0]))).toArray(QueueStatus[]::new);
    }

    @Override
    public String submitBatchJob(JobDescription description) throws XenonException {
        String fullQueueName;
        if (description.getQueueName() == null) {
            fullQueueName = getDefaultQueueName();
        } else {
            fullQueueName = description.getQueueName();
        }
        // Moto accepts arns, but rejects names so translate names to arns
        QueueStatus queue = getQueueStatus(fullQueueName);
        SubmitJobRequest submitJobRequest = AWSBatchUtils.mapToSubmitJobRequest(this, description, queue);

        SubmitJobResult result = client.submitJob(submitJobRequest);
        return result.getJobId();
    }

    @Override
    public Streams submitInteractiveJob(JobDescription description) throws XenonException {
        throw new UnsupportedOperationException(getAdaptorName(), "AWS Batch does not support submitInteractiveJob");
    }

    @Override
    public JobStatus getJobStatus(String jobIdentifier) throws XenonException {
        if (jobIdentifier == null) {
            throw new IllegalArgumentException("jobidentifier can not be null");
        }
        DescribeJobsRequest request = new DescribeJobsRequest().withJobs(jobIdentifier);
        DescribeJobsResult result = client.describeJobs(request);
        if (result.getJobs().isEmpty()) {
            throw new NoSuchJobException(getAdaptorName(), jobIdentifier + " not found");
        }
        JobDetail jobResult = result.getJobs().get(0);
        return AWSBatchUtils.mapJobStatus(this, jobResult);
    }

    @Override
    public JobStatus cancelJob(String jobIdentifier) throws XenonException {
        client.terminateJob(new TerminateJobRequest().withJobId(jobIdentifier).withReason("Cancelled by Xenon cancelJob call"));
        return getJobStatus(jobIdentifier);
    }

    @Override
    public JobStatus waitUntilDone(String jobIdentifier, long timeout) throws XenonException {
        assertNonNullOrEmpty(jobIdentifier, "Job identifier cannot be null or empty");

        long deadline = Deadline.getDeadline(timeout);

        JobStatus status = getJobStatus(jobIdentifier);

        // wait until we are done, or the timeout expires
        long pollDelay = properties.getLongProperty(POLL_DELAY_PROPERTY);
        while (!status.isDone() && System.currentTimeMillis() < deadline) {

            if (AWSBatchUtils.sleep(pollDelay)) {
                return status;
            }

            status = getJobStatus(jobIdentifier);
        }
        return status;
    }

    @Override
    public JobStatus waitUntilRunning(String jobIdentifier, long timeout) throws XenonException {
        assertNonNullOrEmpty(jobIdentifier, "Job identifier cannot be null or empty");

        long deadline = Deadline.getDeadline(timeout);

        JobStatus status = getJobStatus(jobIdentifier);

        // wait until we are done, or the timeout expires
        long pollDelay = properties.getLongProperty(POLL_DELAY_PROPERTY);
        while (!(status.isRunning() || status.isDone()) && System.currentTimeMillis() < deadline) {

            if (AWSBatchUtils.sleep(pollDelay)) {
                return status;
            }

            status = getJobStatus(jobIdentifier);
        }

        return status;
    }

    @Override
    public FileSystem getFileSystem() throws XenonException {
        throw new UnsupportedOperationException(getAdaptorName(), "AWSBatch has no filesystem");
    }
}
