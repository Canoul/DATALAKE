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

import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.limit;

@Service
public class AdCampaignSearchServicelmpl implements AdCampaignSearchService {
    private final static Logger LOGGER = LoggerFactory.getLogger(AdCampaignSearchServicelmpl.class);
    private final MongoCollection<Document> collection;

    public AdCampaignSearchServicelmpl(MongoTemplate mongoTemplate) {
        // Accéder à la collection 'ad_campaign_cleaned' dans la base de données 'staging_db'
        this.collection = mongoTemplate.getCollection("ad_campaign_cleaned");
    }

    @Override
    public Collection<Document> searchAdCampaignsByClicks(int minClicks) {
        LOGGER.info("=> Searching ad campaigns with clicks greater than: {}", minClicks);

        try {
            // Création du pipeline pour l'agrégation
            Bson matchStage = match(gt("clicks", minClicks));  // Recherche avec un clic supérieur à minClicks
            Bson limitStage = limit(100);  // Limiter les résultats à 100
            List<Bson> pipeline = List.of(matchStage, limitStage);

            // Log du pipeline généré
            LOGGER.info("Pipeline: {}", pipeline);

            // Exécution de l'agrégation
            List<Document> docs = collection.aggregate(pipeline).into(new ArrayList<>());
            if (docs.isEmpty()) {
                throw new EntityNotFoundException("No ad campaigns found with clicks greater than: " + minClicks);
            }

            return docs;
        } catch (Exception e) {
            LOGGER.error("Error executing the search for ad campaigns with clicks > " + minClicks, e);
            throw e;  // Relance l'exception après l'avoir loguée
        }
    }
}
