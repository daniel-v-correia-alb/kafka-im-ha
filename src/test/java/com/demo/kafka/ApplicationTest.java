package com.demo.kafka;

import org.testng.annotations.Test;

/**
 * The class ApplicationTest contains the tests to exercise the
 * {@link Application} class
 */
public class ApplicationTest {
    /**
     * This test configures the {@link Application} class
     * to run as a producer sending message to the topic named mytopic.

     * This test is really more of an application runner in that there
     * are no assertions made. Rather you can run the test and then
     * check the file logging.log for output.
     *
     * @throws Exception the exception
     */
    @Test (enabled=false)
    public void canRunApplicationForProducer() throws Exception {
        Application.main(new String[] {"producer","mycooltopic", "10"});
    }

    /**
     * This test configures the {@link Application} class
     * to run as a consumer retrieving message to the topic named mytopic

     * This test is really more of an application runner in that there
     * are no assertions made. Rather you can run the test and then
     * check the file logging.log for output.
     *
     * @throws Exception the exception
     */
    @Test (enabled=false)
    public void canRunApplicationForConsumer() throws Exception {
        Application.main(new String[] {"consumer","mycooltopic", "10"});
    }

}
