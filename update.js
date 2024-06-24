const express = require('express');
const axios = require('axios');
const { MongoClient } = require('mongodb');

const app = express();
const port = 3000;

const axiosInstance = axios.create({
  retry: {
    retries: 3,
    retryDelay: 1000,
  },
});

const selfPing = async () => {
  try {
    await axiosInstance.get('https://eat-market.onrender.com/update-products');
    console.log(`Self-ping successful at ${new Date().toISOString()}`);
  } catch (error) {
    console.error(`Self-ping failed: ${error.message}`);
  }
};

const generateRequestOptions = (productId, lang) => ({
  method: 'GET',
  url: `https://www.carrefouregypt.com/api/v4/relevance/products/${productId}`,
  params: {
    lang,
    placements: 'personal_page.echo_seed|item_page.frequently_bought_together_web',
    displayCurr: 'EGP',
    latitude: '29.967909028696003',
    longitude: '31.266225954206813',
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
  // const categories = productInfo.category.map(cat => ({
  //   name: cat.name,
  //   level: cat.level,
  // }));

  return {
    id: productInfo.id,
    ean: productInfo.ean,
    categoryId: productInfo.category.id,
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
  const collection = db.collection('products');
  const updateData = Object.fromEntries(
    Object.entries(newData).filter(([_, v]) => v != null)
  );
  delete updateData.id;

  await collection.updateOne({ _id: newData.id }, { $set: updateData });
};

let lastUpdatedProductId = 0;
let totalProducts = 0;
let updatedProductsCount = 0;

const updateAllProducts = async () => {
  console.log(`start-update at ${new Date().toISOString()}`);
  const url = 'mongodb+srv://islam:islam@cluster0.l49eyh6.mongodb.net/';
  const dbName = 'carrefouregypt';
  let client;

  try {
    client = await MongoClient.connect(url);
    const db = client.db(dbName);
    const collection = db.collection('products');
    const products = await collection.find().toArray();

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
  } catch (error) {
    console.error('Error connecting to MongoDB:', error);
  } finally {
    if (client) {
      await client.close();
    }
  }
};

updateAllProducts();
setInterval(updateAllProducts, 24 * 60 * 60 * 1000); // Update once per day
setInterval(selfPing, 10 * 60 * 1000); // Self-ping every 10 minutes

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
