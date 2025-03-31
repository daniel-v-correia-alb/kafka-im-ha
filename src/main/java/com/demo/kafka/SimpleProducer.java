package com.demo.kafka;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import com.alticelabs.ccp.exagon.infra_lib.utils.serdes.SerDesUtils;
import com.demo.kafka.records.IsolationReference;
import com.demo.kafka.records.SagaFinished;
import com.demo.kafka.records.SagaStarted;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.Config;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.json.simple.JSONObject;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;


/**
 * The type SimpleProducer is a wrapper class for {@link KafkaProducer}.
 * The object publishes methods that send messages that have random string
 * content onto the Kafka broker defined in {@link /src/resources/config.properties}
 */
class SimpleProducer extends AbstractSimpleKafka {


    private KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean closed = new AtomicBoolean(false);


    private final Logger log = Logger.getLogger(SimpleProducer.class.getName());

    /**
     * Instantiates a new Abstract class, SimpleKafka.
     * <p>
     * This abstract class's constructor provides graceful
     * shutdown behavior for Kafka producers and consumers
     *
     * @throws Exception the exception
     */
    public SimpleProducer() throws Exception {
    }

    /**
     * This method sends a limited number of messages
     * with random string data to the Kafka broker.

     * This method is provided for testing purposes.
     *
     * @param topicName the name of the topic to where messages
     *                  will be sent
     * @param numberOfMessages the number of messages to send
     * @throws Exception the exception that gets raised upon error
     */
    public void run(String topicName, int numberOfMessages) throws Exception {
        int i = 0;
        while (i <= numberOfMessages) {
            String key = UUID.randomUUID().toString();
            String message = MessageHelper.getRandomString();
            this.send(topicName, key, message);
            i++;
            Thread.sleep(100);
        }
        this.shutdown();
    }

    /**
     * This method sends and consumes records from
     * Exagon's Isolation Manager topics to perform
     * tests on the current implementation for HA.
     * -
     * This method tests
     * <a href="https://alabs.atlassian.net/wiki/spaces/PCFKERNEL/pages/508363693/IM+HA+QA-Testing">Scenario A</a>.
     *
     * @param topicName the name of the topic to where messages
     *                  will be sent
     * @param numberOfMessages the number of messages to send
     * @throws Exception the exception that gets raised upon error
     */
    public String runA(String topicName, int numberOfMessages) throws Exception {
        Map<String, IsolationReference> mapActiveSagas = new HashMap<>();
        Map<String, IsolationReference> mapTestSpecificSagas = new HashMap<>();
        ObjectMapper objectMapper = SerDesUtils.getObjectMapper();
        int sagaTimeout = 100000;
        Instant tsReference;
        String podIp;

        log.info("Start Sagas With Some Entropy");
        startSagas(mapActiveSagas, numberOfMessages, topicName, sagaTimeout, null);
        log.info("Finish Half of Active Sagas");
        finishSagas(mapActiveSagas, numberOfMessages/2, topicName);
        log.info("Sending Last SagaStarted");
        tsReference = startSagas(mapTestSpecificSagas, 1, topicName, sagaTimeout, null);
        log.info("Finding the Isolation Manager Master Pod");
        podIp = kafkaFindPartitionConsumerIp("localhost:19092", "exgRebalanced", 0);
        log.info("Killing Isolation Manager Master Pod");
        kubernetesFindPodNameByIpAndDeleteIt(podIp);
        log.info("Sending a SagaStarted With Same tsReference as Last SagaStarted");
        startSagas(mapTestSpecificSagas, 1, topicName, sagaTimeout, tsReference);
        log.info("Producing Two SagaFinished For The Precious Two SagaStarted");
        finishSagas(mapTestSpecificSagas, 2, topicName);
        log.info("Finish The Other Half of Active Sagas");
        finishSagas(mapActiveSagas, numberOfMessages/2, topicName);

        this.shutdown();
        return objectMapper.writeValueAsString(tsReference);
    }

    /**
     * The runAlways method sends a message to a topic.
     *
     * @param topicName    the name of topic to access
     * @param callback the callback function that processes messages retrieved
     *                 from Kafka
     * @throws Exception the Exception that will get thrown upon an error
     */
    public void runAlways(String topicName, KafkaMessageHandler callback) throws Exception {
        while (true) {
            String key = UUID.randomUUID().toString();
            //use the Message Helper to get a random string
            String message = MessageHelper.getRandomString();
            //send the message
            this.send(topicName, key, message);
            Thread.sleep(100);
        }
    }

    private String topicName = null;
    private void setTopicName(String topicName) {
        this.topicName = topicName;
    }
    private String getTopicName() {
        return this.topicName;
    }


    /**
     * Does the work of sending a message to
     * a Kafka broker. The method uses the name of
     * the topic that was declared in this class's
     * constructor.
     *
     * @param topicName the name of the topic to where the message                   will be sent
     * @param key       the key value for the message
     * @param message   the content of the message
     * @throws Exception the exception that gets thrown upon error
     */
    protected void send(String topicName, String key, String message) throws Exception {
        String source = SimpleProducer.class.getName();

        //create the ProducerRecord object which will
        //represent the message to the Kafka broker.
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, key, message);

        //Use the helper to create an informative log entry in JSON format
        JSONObject obj = MessageHelper.getMessageLogEntryJSON(source, topicName, key, message);
        log.info(obj.toJSONString());
        //Send the message to the Kafka broker using the internal
        //KafkaProducer
        getKafkaProducer().send(producerRecord);
    }

    /**
     * This method sends nSagas records to Isolation Manager
     * kafka topic topicPath + SagaStarted and adds these
     * sagas to mapActiveSagas.

     * This method emulates an orchestrator producer when
     * a new Saga is Started
     *
     * @param mapActiveSagas a map of active sagas, i.e., no SagaFinished was generated for these sagas
     * @param nSagas the number of SagaStarted records to produce
     * @param topicPath the Isolation Manager topic path
     * @param sagaTimeout the SagaStarted timeout value
     * @param tsReference the tsReference of the sagas to start
     *
     * @return the last sagaStarted tsReference
     *
     * @throws Exception the exception that gets raised upon error
     */
    private Instant startSagas(Map<String, IsolationReference> mapActiveSagas, int nSagas,
                               String topicPath, int sagaTimeout, Instant tsReference)
            throws Exception {
        ObjectMapper objectMapper = SerDesUtils.getObjectMapper();
        Instant timestamp = null;
        int i = 0;

        while (i < nSagas) {
            String key = UUID.randomUUID().toString();
            if (tsReference == null) {
                timestamp = Instant.now();
            } else {
                timestamp = tsReference;
            }
            SagaStarted sagaStart = new SagaStarted(key, timestamp, sagaTimeout);
            String sagaJson = objectMapper.writeValueAsString(sagaStart);
            this.send(topicPath + ".SagaStarted", key, sagaJson);
            mapActiveSagas.put(key, null);
            i++;
        }
        return timestamp;
    }

    /**
     * This method sends nSagas records to Isolation Manager
     * kafka topic topicPath + SagaFinished and removes these
     * sagas to mapActiveSagas.

     * This method emulates an orchestrator producer when
     * a new Saga is Finished
     *
     * @param mapActiveSagas a map of active sagas, i.e., no SagaFinished was generated for these sagas
     * @param nSagas the number of SagaStarted records to produce
     * @param topicPath the Isolation Manager topic path
     *
     * @throws Exception the exception that gets raised upon error
     */
    private void finishSagas(Map<String, IsolationReference> mapActiveSagas, int nSagas, String topicPath)
            throws Exception {
        ObjectMapper objectMapper = SerDesUtils.getObjectMapper();
        Iterator<String> activeSagasIterator = mapActiveSagas.keySet().iterator();
        int i = 0;

        while (i < nSagas && activeSagasIterator.hasNext()) {
            String sagaId = activeSagasIterator.next();
            SagaFinished sagaFinish = new SagaFinished(sagaId);
            String sagaJson = objectMapper.writeValueAsString(sagaFinish);
            this.send(topicPath + ".SagaFinished", sagaId, sagaJson);
            activeSagasIterator.remove();
            i++;
        }
    }

    /**
     * This method checks which pod of Isolation Manager
     * is the master one by checking the one that is
     * subscribed to partition 0 of topic exgRebalanced.
     *
     * @param bootstrapServers the kafka bootstrap servers
     * @param topicName the topic to find the specified pod subscribed
     * @param partitionToFind the partition to find the pod subscribed
     *
     *
     * @return The ip of the pod subscribed to the topicName and partitionToFind, or null if none is subscribed
     */
    private String kafkaFindPartitionConsumerIp(String bootstrapServers, String topicName, int partitionToFind)
            throws InterruptedException {
        try (AdminClient adminClient = AdminClient.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            List<String> consumerGroups = new ArrayList<>();
            for (ConsumerGroupListing groupListing : adminClient.listConsumerGroups().all().get()) {
                consumerGroups.add(groupListing.groupId());
            }

            for (String groupId : consumerGroups) {
                DescribeConsumerGroupsResult consumerGroupsResult = adminClient.describeConsumerGroups(List.of(groupId));
                Map<String, ConsumerGroupDescription> groupDescriptions = consumerGroupsResult.all().get();

                for (ConsumerGroupDescription groupDescription : groupDescriptions.values()) {
                    for (MemberDescription member : groupDescription.members()) {
                        MemberAssignment assignment = member.assignment();
                        for (TopicPartition partition : assignment.topicPartitions()) {
                            if (partition.topic().equals(topicName) && partition.partition() == partitionToFind) {
                                log.info(member.host());
                                return member.host().replaceAll("/", "");
                            }
                        }
                    }
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("Couldn't find the Isolation Manager Pod Master");
    }

    /**
     * This uses kubernetes to find
     * the pod name with the provided ip
     * and tries to delete it.
     *
     * @param podIp the kubernetes pod ip to delete
     */
    private void kubernetesFindPodNameByIpAndDeleteIt(String podIp) throws IOException, ApiException {
        ApiClient client = Config.defaultClient(); // Uses in-cluster config if running inside Kubernetes
        Configuration.setDefaultApiClient(client);
        CoreV1Api api = new CoreV1Api();
        String podName = null;

        V1PodList podList = api.listPodForAllNamespaces().execute();
        for (V1Pod pod : podList.getItems()) {
            if (pod.getStatus().getPodIP() != null && pod.getStatus().getPodIP().equals(podIp)) {
                podName = pod.getMetadata().getName();
                break;
            }
        }

        if (podName == null) {
            log.error("Couldn't get the name of the pod with the provided ip");
        }
        api.deleteNamespacedPod(podName, System.getenv("NAMESPACE"));
    }

    private KafkaProducer<String, String> getKafkaProducer() throws Exception {
        if (this.kafkaProducer == null) {
            Properties props = PropertiesHelper.getProperties();
            this.kafkaProducer = new KafkaProducer<>(props);
        }
        return this.kafkaProducer;
    }

    public void shutdown() throws Exception {
        closed.set(true);
        log.info(MessageHelper.getSimpleJSONObject("Shutting down producer"));
        getKafkaProducer().close();
    }
}
