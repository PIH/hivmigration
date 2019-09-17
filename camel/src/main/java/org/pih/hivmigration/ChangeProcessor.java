package org.pih.hivmigration;

import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process changes from the HIV EMR DB
 */
public class ChangeProcessor implements Processor {

    static Logger log = LoggerFactory.getLogger(ChangeProcessor.class);

    @Override
    public void process(Exchange msg) {
        log.trace("Processing msg {}", msg);
        Map<String, Object> record = msg.getIn().getBody(Map.class);
        log.info("Processing record {}", record);
    }
}
