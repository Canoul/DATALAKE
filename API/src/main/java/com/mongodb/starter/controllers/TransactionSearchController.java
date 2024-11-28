package com.mongodb.starter.controllers;

import com.mongodb.starter.services.TransactionSearchService;
import com.mongodb.starter.services.SocialDataSearchService;  
import com.mongodb.starter.services.AdCampaignSearchService;  
import com.mongodb.starter.exception.EntityNotFoundException;  
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;

@RestController
@RequestMapping("/transactions")    
public class TransactionSearchController {

    private final static Logger LOGGER = LoggerFactory.getLogger(TransactionSearchController.class);

    private final TransactionSearchService transactionSearchService;
    private final SocialDataSearchService socialDataSearchService;  
    private final AdCampaignSearchService adCampaignSearchService;  

    public TransactionSearchController(TransactionSearchService transactionSearchService, 
                                       SocialDataSearchService socialDataSearchService, 
                                       AdCampaignSearchService adCampaignSearchService) {
        this.transactionSearchService = transactionSearchService;
        this.socialDataSearchService = socialDataSearchService;  
        this.adCampaignSearchService = adCampaignSearchService;  
    }

    @GetMapping("/search/{paymentMethod}")
    public Collection<Document> getTransactionsByPaymentMethod(@PathVariable String paymentMethod,
                                                                @RequestParam(value = "limit", defaultValue = "5") int limit) {
        LOGGER.info("Fetching transactions with payment method: {} and limit: {}", paymentMethod, limit);
        return transactionSearchService.searchTransactionsByPaymentMethod(paymentMethod, limit);
    }

    @GetMapping("/social/search/{userId}")
    public Collection<Document> getSocialDataByUserId(@PathVariable String userId,
                                                      @RequestParam(value = "limit", defaultValue = "5") int limit) {
        LOGGER.info("Fetching social data for user ID: {} with limit: {}", userId, limit);
        return socialDataSearchService.searchByUserId(userId, limit);  
    }

    @GetMapping("/adcampaign/search")
    public Collection<Document> getAdCampaignsWithHighClicks(@RequestParam(value = "minClicks", defaultValue = "50") int minClicks) {
        LOGGER.info("Fetching ad campaigns with clicks greater than: {}", minClicks);
        return adCampaignSearchService.searchAdCampaignsByClicks(minClicks);
    }

    @ExceptionHandler(EntityNotFoundException.class)
    @ResponseStatus(value = HttpStatus.NOT_FOUND, reason = "MongoDB didn't find any document.")
    public final void handleNotFoundExceptions(EntityNotFoundException e) {
        LOGGER.warn("Entity not found: {}", e.getMessage());
    }

    @ExceptionHandler(RuntimeException.class)
    @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR, reason = "Internal Server Error")
    public final void handleAllExceptions(RuntimeException e) {
        LOGGER.error("Internal server error.", e);
    }
}
