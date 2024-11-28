package com.mongodb.starter.services;

import com.mongodb.client.MongoCollection;
import com.mongodb.starter.exception.EntityNotFoundException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.limit;

@Service
public class TransactionSearchServicelmpl implements TransactionSearchService {
    private final static Logger LOGGER = LoggerFactory.getLogger(TransactionSearchServicelmpl.class);
    private final MongoCollection<Document> collection;

    public TransactionSearchServicelmpl(MongoTemplate mongoTemplate) {
        this.collection = mongoTemplate.getCollection("transactions_cleaned");
    }

    @Override
    public Collection<Document> searchTransactionsByPaymentMethod(String paymentMethod, int limitValue) {
        LOGGER.info("=> Searching transactions by payment method: {} with limit {}", paymentMethod, limitValue);

        try {
            // Création du pipeline pour l'agrégation
            Bson matchStage = match(eq("payment_method", paymentMethod));
            Bson limitStage = limit(limitValue);
            List<Bson> pipeline = List.of(matchStage, limitStage);

            // Log du pipeline généré
            LOGGER.info("Pipeline: {}", pipeline);

            List<Document> docs = collection.aggregate(pipeline).into(new ArrayList<>());
            if (docs.isEmpty()) {
                throw new EntityNotFoundException("No transactions found for payment method: " + paymentMethod);
            }

            return docs;
        } catch (Exception e) {
            LOGGER.error("Error executing the search for payment method: " + paymentMethod, e);
            throw e;  // Relance l'exception après l'avoir loguée
        }
    }
}
