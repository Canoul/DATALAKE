package com.mongodb.starter.services;

import org.bson.Document;
import java.util.Collection;

public interface SocialDataSearchService {
    Collection<Document> searchByUserId(String userId, int limitValue);
}
