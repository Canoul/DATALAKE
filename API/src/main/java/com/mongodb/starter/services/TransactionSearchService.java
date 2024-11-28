package com.mongodb.starter.services;

import org.bson.Document;
import java.util.Collection;

public interface TransactionSearchService {
    // Définir la méthode dans l'interface
    Collection<Document> searchTransactionsByPaymentMethod(String paymentMethod, int limit);
}
