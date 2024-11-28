package com.mongodb.starter.services;

import org.bson.Document;
import java.util.Collection;

public interface AdCampaignSearchService {
    Collection<Document> searchAdCampaignsByClicks(int minClicks);
}
