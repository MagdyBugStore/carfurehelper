const axios = require('axios');
const { MongoClient } = require('mongodb');
const express = require('express');

const BATCH_SIZE = 1000;
const API_URL = 'https://www.carrefouregypt.com/api/v4/products';
const COORDINATES = { latitude: 29.967909028696003, longitude: 31.266225954206813 };
const MONGO_URL = 'mongodb+srv://islam:islam@cluster0.l49eyh6.mongodb.net/';
const DB_NAME = 'carrefouregypt';
const IDS_COLLECTION = 'productIds';
const PRODUCTS_COLLECTION = 'products';

const app = express();
const port = 3000;

const mongoClient = new MongoClient(MONGO_URL);
const axiosInstance = axios.create({
    retry: { retries: 3, retryDelay: 1000 },
});

const fetchIds = async (skip, limit) => {
    return Array.from({ length: Math.min(limit, 1000000 - skip) }, (_, i) => skip + i + 1);
};

const fetchProductData = async (ids, lang) => {
    try {
        const { data } = await axios.get(API_URL, {
            params: { ids: ids.join(','), lang, displayCurr: 'EGP', ...COORDINATES },
            headers: {
                'Host': 'www.carrefouregypt.com',
                'Storeid': 'mafegy',
                'Content-Length': 0,
            },
        });
        return data.data.products.map(product => ({ _id: product.id }));
    } catch (error) {
        console.error(`Error fetching product data (${lang}):`, error);
        return [];
    }
};

const fetchAndProcessIdsInBatches = async (client) => {
    const idsCollection = client.db(DB_NAME).collection(IDS_COLLECTION);

    for (let skip = 0; ; skip += BATCH_SIZE) {
        const ids = await fetchIds(skip, BATCH_SIZE);
        if (!ids.length) break;

        console.log(`Processing batch with ${ids.length} IDs...`);
        const enProducts = await fetchProductData(ids, 'en');
        if (enProducts.length > 0) {
            try {
                await idsCollection.insertMany(enProducts, { ordered: false });
            } catch (err) {
                if (err.code !== 11000) throw err;
            }
        } else {
            console.log('No products to insert in this batch.');
        }
    }
};

const generateRequestOptions = (productId, lang) => ({
    method: 'GET',
    url: `https://www.carrefouregypt.com/api/v4/relevance/products/${productId}`,
    params: {
        lang,
        placements: 'personal_page.echo_seed|item_page.frequently_bought_together_web',
        displayCurr: 'EGP',
        ...COORDINATES,
    },
    headers: {
        Host: 'www.carrefouregypt.com',
        Storeid: 'mafegy',
        'Content-Length': '4',
    },
});

const extractProductData = ({ data }, lang) => {
    const productInfo = data.placements?.[0]?.recommendedProducts?.[0];
    if (!productInfo) return null;

    const images = productInfo.links.images.map(image => image.href);
    const thumbnail = images.find(image => image.includes('_200Wx200H'));
    const { defaultImages } = productInfo.links;
    const categories = productInfo.category.map(cat => ({ name: cat.name, level: cat.level }));

    return {
        id: productInfo.id,
        ean: productInfo.ean,
        [`category_${lang}`]: categories,
        [`name_${lang}`]: productInfo.name,
        brand: productInfo.brand?.name || null,
        supplier: productInfo.supplier || null,
        availability: productInfo.availability.isAvailable,
        price: productInfo.price.price,
        discount: productInfo.price.discount?.price || null,
        stockLevelStatus: productInfo.stock.stockLevelStatus,
        productUrl: productInfo.links.productUrl.href,
        productPhotos: { thumbnail, defaultImages },
    };
};

const updateProductInMongoDB = async (db, newData) => {
    const collection = db.collection(PRODUCTS_COLLECTION);
    const updateData = Object.fromEntries(Object.entries(newData).filter(([_, v]) => v != null));
    delete updateData.id;
    await collection.updateOne({ _id: newData.id }, { $set: updateData });
};

let lastUpdatedProductId = 0;
let totalProducts = 0;
let updatedProductsCount = 0;

const updateAllProducts = async (client) => {
    console.log(`start-update at ${new Date().toISOString()}`);
    const db = client.db(DB_NAME);
    const products = await db.collection(IDS_COLLECTION).find().toArray();

    totalProducts = products.length;
    updatedProductsCount = 0;

    for (const product of products) {
        try {
            const [responseEn, responseAr] = await Promise.all([
                axiosInstance(generateRequestOptions(product._id, 'en')),
                axiosInstance(generateRequestOptions(product._id, 'ar')),
            ]);

            const productDataEn = extractProductData(responseEn.data, 'en');
            const productDataAr = extractProductData(responseAr.data, 'ar');
            if (!productDataEn || !productDataAr) continue;

            const mergedProductData = { ...productDataEn, ...productDataAr };
            await updateProductInMongoDB(db, mergedProductData);
            lastUpdatedProductId = product._id;
            updatedProductsCount++;
            console.log(`Successfully updated product ID ${product._id}`);
        } catch (error) {
            console.error(`Error fetching data for product ID ${product._id}: ${error.message}`);
        } finally {
            await new Promise(resolve => setTimeout(resolve, 400));
        }
    }
};

const scheduleDailyTasks = async (client) => {
    await fetchAndProcessIdsInBatches(client);
    await updateAllProducts(client);
};

(async () => {
    try {
        await mongoClient.connect();
        console.log('Connected to MongoDB');

        scheduleDailyTasks(mongoClient);
        setInterval(() => scheduleDailyTasks(mongoClient), 24 * 60 * 60 * 1000); // Run once per day
        setInterval(async () => {
            try {
                await axiosInstance.get('http://localhost:3000/update-products');
                console.log(`Self-ping successful at ${new Date().toISOString()}`);
            } catch (error) {
                console.error(`Self-ping failed: ${error.message}`);
            }
        }, 10 * 60 * 1000); // Self-ping every 10 minutes

        app.get('/update-products', (req, res) => {
            res.send(`Last updated product ID: ${lastUpdatedProductId}, Products left: ${totalProducts - updatedProductsCount}, Products updated: ${updatedProductsCount}`);
        });

        app.use((err, req, res, next) => {
            console.error('Error:', err.message);
            res.status(500).send('Something went wrong. Please try again later.');
        });

        app.listen(port, () => {
            console.log(`Server listening at http://localhost:${port}`);
        });
    } catch (error) {
        console.error('Failed to connect to MongoDB:', error);
    }
})();
