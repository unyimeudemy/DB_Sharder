package com.piraxx.sharder.sharderPackage.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceCloser {

    private static final Logger logger = LoggerFactory.getLogger(ResourceCloser.class);


    // NOTE try-with-resources statement can eliminate the need for this
    public static void closeResources(AutoCloseable ... resources){
        for (AutoCloseable resource: resources){
            try{
                if(resource != null){
                    resource.close();
                    logger.info("{} closed successfully.", resource.getClass().getSimpleName());
                }
            } catch (Exception ex){
                logger.error("Error closing resource: {}", resource.getClass().getSimpleName(), ex);
            }
        }
    }
}
