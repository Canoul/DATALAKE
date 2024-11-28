package com.mongodb.starter.services;

import com.mongodb.client.MongoCollection;
import com.mongodb.starter.exception.EntityNotFoundException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.limit;

@Service
public class SocialDataSearchServicelmpl implements SocialDataSearchService {

    private final static Logger LOGGER = LoggerFactory.getLogger(SocialDataSearchServicelmpl.class);
    private final MongoCollection<Document> collection;

    public SocialDataSearchServicelmpl(MongoTemplate mongoTemplate) {
        this.collection = mongoTemplate.getCollection("social_data_cleaned");
    }

    @Override
    public Collection<Document> searchByUserId(String userId, int limitValue) {
        LOGGER.info("=> Searching social data by user_id: {} with limit {}", userId, limitValue);
        try {
            Bson matchStage = match(eq("user_id", userId));
            Bson limitStage = limit(limitValue);
            List<Bson> pipeline = List.of(matchStage, limitStage);

            LOGGER.info("Pipeline: {}", pipeline);

            List<Document> docs = collection.aggregate(pipeline).into(new ArrayList<>());
            if (docs.isEmpty()) {
                throw new EntityNotFoundException("No social data found for user_id: " + userId);
            }

            return docs;
        } catch (Exception e) {
            LOGGER.error("Error executing the search for user_id: " + userId, e);
            throw e;
        }
    }
}
