package com.demo.kafka;

import com.alticelabs.ccp.exagon.infra_lib.utils.serdes.SerDesUtils;
import com.demo.kafka.records.IsolationReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.time.Instant;

/**
 * The type Application.
 */
public class Application {
    
    /**
     * The Isolation Manager kafka topic IsolationReference.
     */
    private static final String topicIsolationReference =
            "com.alticelabs.ccp.exagon.infra_lib.isolation_manager.models.IsolationReference";

    /**
     * The Scenario A saga's tsReference.
     */
    private static String scenarioAtsReference;

    private static class ApplicationMessageHandlerImpl implements KafkaMessageHandler{

        /**
         * The Log.
         */
        static Logger log = Logger.getLogger(ApplicationMessageHandlerImpl.class.getName());

        /**
         * First consumed record with scenario A tsReference.
         */
        IsolationReference scenarioAiR = null;

        @Override
        public void processMessage(String topicName, ConsumerRecord<String, String> message) throws Exception {
            String source = KafkaMessageHandlerImpl.class.getName();
            JSONObject obj = MessageHelper.getMessageLogEntryJSON(source, topicName,message.key(),message.value());
            System.out.println(obj.toJSONString());
            log.info(obj.toJSONString());
            /* If record was consumed from other topic than IsolationReference topic */
            if (!topicName.equals(topicIsolationReference)) {
                return;
            }
            
            ObjectMapper objectMapper = SerDesUtils.getObjectMapper();
            IsolationReference iR = objectMapper.readValue(message.value(), IsolationReference.class);
            Instant aTestTsReference = objectMapper.readValue(scenarioAtsReference, Instant.class);
            /* If IsolationReference record does not have scenario A's tsReference */
            if (iR.getTsEventReference().compareTo(aTestTsReference) != 0) {
                return;
            }
            /* If we consumed first record with scenario A's tsReference */
            if (scenarioAiR == null) {
                scenarioAiR = iR; return;
            }

            String testInfoSaga1 = "Scenario A - First record:/n  tsRead - " + scenarioAiR.getTsRead() + "/n  tsWrite - " +
                    scenarioAiR.getTsWrite() + "/n  tsReference - " + scenarioAiR.getTsEventReference() + "/n";
            String testInfoSaga2 = "Scenario A - Second record:/n  tsRead - " + iR.getTsRead() + "/n  tsWrite - " + iR.getTsWrite()
                    + "/n  tsReference - " + iR.getTsEventReference() + "/n";

            log.info(testInfoSaga1);
            log.info(testInfoSaga2);
            /* If we consumed second record with scenario A's tsReference. Compare iR's */
            if (iR.getTsRead().compareTo(scenarioAiR.getTsRead()) > 0 ||
                    iR.getTsWrite().compareTo(scenarioAiR.getTsWrite()) != 0 ||
                    iR.getTsEventReference().compareTo(scenarioAiR.getTsEventReference()) != 0)  {
                throw new Exception(testInfoSaga1 + testInfoSaga2);
            }

            scenarioAiR = null;
        }
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws Exception the exception
     */
    public static void main(String[] args) throws Exception {
        String errorStr = "ERROR: You need to declare the first parameter as Producer or Consumer, " +
                "the second parameter is the topic name, and the third parameter as the number of records";

        if (args.length != 3) {
            System.out.println(errorStr);
            return;
        }

        ApplicationMode mode = ApplicationMode.valueOf(args[0]);
        String topicName = args[1];
        int numberOfRecords = Integer.parseInt(args[2]);
        switch(mode) {
            case PRODUCER:
                System.out.println("Starting the Producer\n");
                new SimpleProducer().run(topicName, numberOfRecords);
                break;
            case CONSUMER:
                System.out.println("Starting the Consumer\n");
                new SimpleConsumer().runAlways(topicName, new ApplicationMessageHandlerImpl() );
                break;
            case IM_HA_SCENARIO_A:
                System.out.println("Starting Producer for Scenario A Test for Isolation Manager\n");
                scenarioAtsReference = new SimpleProducer().runA(topicName, numberOfRecords);

                System.out.println("Starting Consumer for Scenario A Test for Isolation Manager\n");
                new SimpleConsumer().run(topicIsolationReference, new ApplicationMessageHandlerImpl(), null);
                break;
            default:
                System.out.println(errorStr);
        }


    }
}
