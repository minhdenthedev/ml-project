/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.movielens.service;

import com.movielens.repositories.HiveRepository;
import java.io.File;
import java.util.List;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.springframework.beans.factory.annotation.Autowired;

/**
 *
 * @author minh
 */
public class Recommender {

    public static List<RecommendedItem> getUserBasedRecommendations(int userId, int number) {
        try {
            File file = new File("/home/minh/datasets/ml-100k/u_data.txt");
            DataModel dataModel = new FileDataModel(file);
            UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(dataModel);

            UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(3, userSimilarity, dataModel);

            org.apache.mahout.cf.taste.recommender.Recommender userBasedRecommender = new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);

            org.apache.mahout.cf.taste.recommender.Recommender cachingRecommender = new CachingRecommender(userBasedRecommender);

            List<RecommendedItem> recommendations = cachingRecommender.recommend(userId, number);

            return recommendations;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static List<RecommendedItem> getItemBasedRecommendations(int itemId, int number) throws TasteException {
        try {
            DataModel dataModel = new FileDataModel(new File("/home/minh/datasets/ml-100k/u_data.txt"));
            // Construct the list of pre-computed correlations
            ItemBasedRecommender recommender = new GenericItemBasedRecommender(dataModel,
                    new PearsonCorrelationSimilarity(
                            dataModel));
            List<RecommendedItem> similarItems = recommender.mostSimilarItems(itemId, number);
            
            return similarItems;
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return null;
    }
}
