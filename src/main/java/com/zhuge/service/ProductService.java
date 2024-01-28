package com.zhuge.service;

import com.alibaba.fastjson.JSON;
import com.zhuge.common.RedisKeyPrefixConst;
import com.zhuge.common.RedisUtil;
import com.zhuge.dao.ProductDao;
import com.zhuge.model.Product;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RReadWriteLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Service
public class ProductService {

    @Autowired
    private ProductDao productDao;

    @Autowired
    private RedisUtil redisUtil;

    @Autowired
    private Redisson redisson;

    public static final Integer PRODUCT_CACHE_TIMEOUT = 60 * 60 * 24;
    public static final String EMPTY_CACHE = "{}";
    public static final String LOCK_PRODUCT_HOT_CACHE_CREATE_PREFIX = "lock:product:hot_cache_create:";
    public static final String LOCK_PRODUCT_UPDATE_PREFIX = "lock:product:update:";
    public static Map<String, Product> productMap = new HashMap<>();

    @Transactional
    public Product create(Product product) {
        Product productResult = productDao.create(product);
        redisUtil.set(RedisKeyPrefixConst.PRODUCT_CACHE + productResult.getId(), JSON.toJSONString(productResult));
        return productResult;
    }

    @Transactional
    public Product update(Product product) {
        Product productResult = null;
        //RLock productUpdateLock = redisson.getLock(LOCK_PRODUCT_UPDATE_PREFIX + product.getId());
//        RLock productUpdateLock = redisson.getLock(LOCK_PRODUCT_UPDATE_PREFIX + product.getId());
        RReadWriteLock productUpdateLock = redisson.getReadWriteLock(LOCK_PRODUCT_UPDATE_PREFIX + product.getId());
        //加分布式写锁解决缓存双写不一致问题
        RLock productUpdateWriteLock = productUpdateLock.writeLock();
        productUpdateWriteLock.lock();
//        productUpdateLock.lock();
        try {
            productResult = productDao.update(product);
            redisUtil.set(RedisKeyPrefixConst.PRODUCT_CACHE + productResult.getId(), JSON.toJSONString(productResult),
                    genProductCacheTimeout(), TimeUnit.SECONDS);
        } finally {
            productUpdateWriteLock.unlock();
        }
        return productResult;
    }

    public Product get(Long productId) {
        Product product;
        String productCacheKey = RedisKeyPrefixConst.PRODUCT_CACHE + productId;

        // get data from cache
        product = getProductFromCache(productCacheKey);
        if (product != null) {
            return product;
        }

        // add distribution lock
        RLock hotCreatedCacheLock = redisson.getLock(LOCK_PRODUCT_HOT_CACHE_CREATE_PREFIX + productId);
        hotCreatedCacheLock.lock();
        try {
            product = getProductFromCache(productCacheKey);
            if (product != null) {
                return product;
            }
            // distribution lock enable 双写一致性
            RReadWriteLock productUpdateLock = redisson.getReadWriteLock(LOCK_PRODUCT_UPDATE_PREFIX + productId);
            RLock productUpdateReadLock = productUpdateLock.readLock();
//            RLock productUpdateLock = redisson.getLock(LOCK_PRODUCT_UPDATE_PREFIX + productId);
            // 添加读锁解决读写一致性问题
            productUpdateReadLock.lock();
            try {
                // set cache data
                setProductCacheFromDB(productId, productCacheKey);
            } finally {
                productUpdateReadLock.unlock();
            }
        } finally {
            hotCreatedCacheLock.unlock();
        }

        // set cache data
        setProductCacheFromDB(productId, productCacheKey);

        return product;
    }


    private Integer genProductCacheTimeout() {
        //加随机超时机制解决缓存批量失效(击穿)问题
        return PRODUCT_CACHE_TIMEOUT + new Random().nextInt(5) * 60 * 60;
    }

    private Integer genEmptyCacheTimeout() {
        return 60 + new Random().nextInt(30);
    }

    private Product getProductFromCache(String productCacheKey) {
        Product product = null;
        //从缓存里查数据
        String productStr = redisUtil.get(productCacheKey);
        // 缓存中存在数据
        if (!StringUtils.isEmpty(productStr)) {
            // 空缓存，返回空商品并延期
            if (EMPTY_CACHE.equals(productStr)) {
                redisUtil.expire(productCacheKey, genEmptyCacheTimeout(), TimeUnit.SECONDS);
                return new Product();
            }
            product = JSON.parseObject(productStr, Product.class);
            redisUtil.expire(productCacheKey, genProductCacheTimeout(), TimeUnit.SECONDS);
        }
        return product;
    }

    private void setProductCacheFromDB(Long productId, String productCacheKey) {
        Product product = null;
        // 击穿到 DB 层
        product = productDao.get(productId);
        if (product != null) {
            redisUtil.set(productCacheKey, JSON.toJSONString(product),
                    genProductCacheTimeout(), TimeUnit.SECONDS);
        } else {
            //设置空缓存解决缓存穿透问题
            redisUtil.set(productCacheKey, EMPTY_CACHE, genEmptyCacheTimeout(), TimeUnit.SECONDS);
        }
    }

}