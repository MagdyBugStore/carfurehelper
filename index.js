const express = require('express');
const axios = require('axios');
const { MongoClient } = require('mongodb');

async function refshreshWebsite() {
  try {
    const response = await axios({
      method: 'GET',
      url: 'https://carfure.onrender.com',
    });
    console.log("web site = ", response.data, " ", Date());
  }
  catch (err) {

  }
}

async function refshreshme() {
  try {
    const response = await axios({
      method: 'GET',
      url: 'https://carfurehelper.onrender.com',
    });
    console.log("web site = ", response.data, " ", Date());
  }
  catch (err) {

  }
}

setInterval(refshreshWebsite, 600000);
setInterval(refshreshme, 600000);

const app = express();
const port = 3010;

const generateRequestOptions = (productId) => ({
  method: 'GET',
  url: `https://www.carrefouregypt.com/api/v4/relevance/products/${productId}`,
  params: {
    lang: 'en',
    placements: 'personal_page.echo_seed|item_page.frequently_bought_together_web',
    displayCurr: 'EGP',
    latitude: '29.967909028696003',
    longitude: '31.266225954206813'
  },
  headers: {
    Host: 'www.carrefouregypt.com',
    Storeid: 'mafegy',
    'Content-Length': '4'
  }
});

const extractProductData = ({ data }) => {
  const productInfo = data.placements?.[0]?.recommendedProducts?.[0];

  if (!productInfo || productInfo.id === "512348") return {};

  const images = productInfo.links.images.map(image => image.href);
  const thumbnail = images.find(image => image.includes('_200Wx200H'));
  const medium = productInfo.links.defaultImages[0];

  const categories = productInfo.category.map(cat => ({
    name: cat.name,
    level: cat.level
  }));

  return {
    id: productInfo.id,
    ean: productInfo.ean,
    category: categories,
    name: productInfo.name,
    brand: productInfo.brand?.name || null,
    supplier: productInfo.supplier || null,
    availability: productInfo.availability.isAvailable,
    price: productInfo.price.price,
    discount: productInfo.price.discount?.price || null,
    stockLevelStatus: productInfo.stock.stockLevelStatus,
    productUrl: productInfo.links.productUrl.href,
    productPhotos: { thumbnail, medium }
  };
};

const removeNullValues = (obj) => {
  const newObj = {};
  for (const key in obj) {
    if (obj[key] !== null && obj[key] !== undefined) {
      newObj[key] = obj[key];
    }
  }
  return newObj;
};

const appendDataToMongoDB = async (db, newData) => {
  try {
    const collection = db.collection('products');
    const updateData = removeNullValues({ ...newData });
    delete updateData.id;

    await collection.updateOne(
      { _id: newData.id },
      { $set: updateData },
      { upsert: true }
    );
    console.log('Product', newData.id, 'saved to MongoDB');
  } catch (error) {
    console.error('Error saving data to MongoDB:', error);
  }
};

let lastProductId = 0;
let lastTry = 0;

const fetchDataAndProcess = async () => {
  const url = 'mongodb+srv://islam:islam@cluster0.l49eyh6.mongodb.net/';
  const dbName = 'carrefouregypt';

  let client;

  try {
    client = await MongoClient.connect(url);
    console.log('Connected to MongoDB');
    const db = client.db(dbName);

    for (let productId = 500168; true; productId++) {
      try {
        const response = await axios(generateRequestOptions(productId));
        const productData = extractProductData(response.data);
        lastTry = productId;
        if (!productData.id) continue;
        await appendDataToMongoDB(db, productData);
        lastProductId = productId;
      } catch (error) {
        console.error(`Error fetching data for product ID ${productId}:`, error);
      } finally {
        await new Promise(resolve => setTimeout(resolve, 250));
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

app.get('/', (req, res) => {
  res.send(`Last try ${lastTry} & Last sucess ${lastProductId}`);
});

app.listen(port, () => {
  console.log(`Server listening at http://localhost:${port}`);
});

fetchDataAndProcess();
