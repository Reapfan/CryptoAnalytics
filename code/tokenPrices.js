const { Client } = require('pg');
const axios = require('axios');
const { setTimeout } = require('timers/promises');

// Конфигурация
const config = {
  db: {
    user: 'your_user',
    host: 'your_host',
    database: 'your_db',
    password: 'your_password',
    port: 5432
  },
  coingecko: {
    apiKey: 'your_key',
    coinId: 'litecoin',
    currencies: ['usd', 'btc'],
    interval: 'hourly',
    chunkDays: 30,
    delayBetweenRequests: 2500
  },
  timeframe: {
    start: new Date('2025-03-01').getTime(),
    end: new Date('2025-03-31').getTime()
  }
};

async function main() {
  console.log('Starting CoinGecko data collection...');
  console.log(`Time range: ${new Date(config.timeframe.start)} - ${new Date(config.timeframe.end)}`);

  try {
    // Получаем данные для всех валют
    const currencyData = await Promise.all(
      config.coingecko.currencies.map(currency =>
        fetchCoinGeckoData(currency)
      )
    );

    // Объединяем данные (первый элемент - USD, второй - BTC)
    const combined = combineData(currencyData[0].prices, currencyData[1].prices);

    await saveToDatabase(combined);
    console.log(`Data collection completed! Saved ${combined.length} records.`);
  } catch (error) {
    console.error('Error in main process:', error.message);
  }
}

// Остальные функции остаются без изменений
async function fetchCoinGeckoData(currency) {
  const results = [];
  let currentStart = config.timeframe.start;

  while (currentStart < config.timeframe.end) {
    const chunkEnd = Math.min(
      currentStart + (config.coingecko.chunkDays * 86400 * 1000),
      config.timeframe.end
    );

    const url = `https://api.coingecko.com/api/v3/coins/${config.coingecko.coinId}/market_chart/range` +
      `?vs_currency=${currency}` +
      `&from=${Math.floor(currentStart / 1000)}` +
      `&to=${Math.floor(chunkEnd / 1000)}`;

    console.log(`Fetching ${currency.toUpperCase()} data: ${new Date(currentStart)} - ${new Date(chunkEnd)}`);

    try {
      const response = await axios.get(url, {
        headers: {
          'accept': 'application/json',
          'x-cg-api-key': config.coingecko.apiKey
        },
        timeout: 10000
      });

      results.push(...response.data.prices);
      await setTimeout(config.coingecko.delayBetweenRequests);
      currentStart = chunkEnd + 1;
    } catch (error) {
      console.error(`Error fetching ${currency} data:`, error.response?.data || error.message);
      throw error;
    }
  }

  return { prices: results };
}

function combineData(usdPrices, btcPrices) {
  const btcMap = new Map(btcPrices.map(item => [item[0], item[1]]));
  const combined = [];

  usdPrices.forEach(usdItem => {
    const timestamp = usdItem[0];
    const btcPrice = btcMap.get(timestamp);

    if (btcPrice) {
      combined.push({
        timestamp: new Date(timestamp),
        price_usdt: usdItem[1],
        price_btc: btcPrice
      });
    }
  });

  return combined;
}

async function saveToDatabase(data) {
  if (data.length === 0) {
    console.log('No data to save');
    return;
  }

  const client = new Client(config.db);

  try {
    await client.connect();
    console.log(`Saving ${data.length} records to database...`);
    await client.query('BEGIN');

    // 1. Создаем временную таблицу без колонки id
    await client.query(`
      CREATE TEMPORARY TABLE temp_token_prices (
        contract_address text,
        token_symbol text,
        timestamp timestamp,
        price_usdt numeric,
        price_btc numeric
      ) ON COMMIT DROP
    `);

    // 2. Вставляем данные во временную таблицу
    const batchSize = 1000;
    for (let i = 0; i < data.length; i += batchSize) {
      const batch = data.slice(i, i + batchSize);
      const values = batch.map(item =>
        `(NULL, 'ltc', '${item.timestamp.toISOString()}', ${item.price_usdt}, ${item.price_btc})`
      ).join(',');

      await client.query(`
        INSERT INTO temp_token_prices
        VALUES ${values}
      `);
    }

    // 3. Вставляем в основную таблицу, явно указывая колонки (кроме id)
    await client.query(`
      INSERT INTO token_prices (contract_address, token_symbol, timestamp, price_usdt, price_btc)
      SELECT contract_address, token_symbol, timestamp, price_usdt, price_btc
      FROM temp_token_prices
      WHERE NOT EXISTS (
        SELECT 1 FROM token_prices 
        WHERE token_prices.token_symbol = temp_token_prices.token_symbol
        AND token_prices.timestamp = temp_token_prices.timestamp
      )
    `);

    await client.query('COMMIT');
    console.log('Data successfully saved!');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Database error:', error);
  } finally {
    await client.end();
  }
}

main().catch(console.error);
