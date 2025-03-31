package com.demo.kafka;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Properties;

/**
 * This class contains the tests run against the
 * {@link PropertiesHelper}
 */
public class PropertiesHelperTest {
    /**
     * Tests that the expected properties are returned by
     * the {@link PropertiesHelper} class
     *
     * @throws Exception the exception
     */
    @Test
    public void testPropertiesValues() throws Exception {
        Properties props = PropertiesHelper.getProperties();
        Assert.assertEquals(props.getProperty("enable.auto.commit").getClass(),String.class);
        Assert.assertEquals(props.getProperty("default.topic").getClass(),String.class);
        Assert.assertEquals(props.getProperty("bootstrap.servers").getClass(),String.class);
        Assert.assertEquals(props.getProperty("key.serializer").getClass(),String.class);
        Assert.assertEquals(props.getProperty("value.serializer").getClass(),String.class);
        Assert.assertEquals(props.getProperty("key.deserializer").getClass(),String.class);
        Assert.assertEquals(props.getProperty("value.deserializer").getClass(),String.class);
        Assert.assertEquals(props.getProperty("group.id").getClass(),String.class);
    }
}
