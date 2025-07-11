const axios = require('axios');
const { Pool } = require('pg');
const { DateTime } = require('luxon');
const { Settings } = require('luxon');
Settings.defaultZone = 'Europe/Moscow';

const config = {
    api: {
        nownodesUrl: 'https://ltcbook.nownodes.io/api/v2',
        nownodesKey: 'your_key',
        delay: 2000,
        maxRetries: 3,
        maxPages: 20
    },
    db: {
        user: 'your_user',
        host: 'your_host',
        database: 'your_db',
        password: 'your_password',
        port: 5432
    },
    blockchain: {
        symbol: 'ltc',
        name: 'Litecoin',
        avgBlockTime: 2.5 * 60,
        confirmations: 6
    },
    processing: {
        batchSize: 50,
        maxParallelRequests: 5,
        dateRange: {
            startDate: '2025-03-01',
            endDate: '2025-03-31'
        }
    }
};
const pool = new Pool(config.db);
const nownodesAxios = axios.create({
    baseURL: config.api.nownodesUrl,
    timeout: 30000,
    headers: { 'api-key': config.api.nownodesKey }
});

let processedWallets = 0;
let processedTransactions = 0;
let totalWallets = 0;
const priceCache = new Map();

const log = (message, level = 'INFO') => {
    console.log(`${DateTime.now().toISO()} - ${level} - ${message}`);
};

const CONFIG_START = DateTime.fromISO(config.processing.dateRange.startDate, { zone: 'utc' });
const CONFIG_END = DateTime.fromISO(config.processing.dateRange.endDate, { zone: 'utc' }).endOf('day');

function isInConfigRange(timestamp) {
    const date = DateTime.fromSeconds(timestamp, { zone: 'utc' });
    const isInRange = date >= CONFIG_START && date <= CONFIG_END;

    if (!isInRange) {
        log(`Timestamp ${timestamp} (${date.toISO()}) is outside range ` +
            `${CONFIG_START.toISO()} to ${CONFIG_END.toISO()}`, 'DEBUG');
    }

    return isInRange;
}

const checkDatabaseConnection = async () => {
    const client = await pool.connect();
    try {
        await client.query('SELECT 1');
        log('Database connection successful', 'INFO');
    } catch (error) {
        log(`Database connection failed: ${error.message}`, 'ERROR');
        throw error;
    } finally {
        client.release();
    }
};
const checkTableStructure = async () => {
    const client = await pool.connect();
    try {
        const res = await client.query(`
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'transactions'
        `);

        const columns = res.rows.map(row => row.column_name);

        if (!columns.includes('usdt_volume') || !columns.includes('btc_volume')) {
            log('Adding missing columns to transactions table', 'INFO');
            await client.query(`
                ALTER TABLE transactions 
                ADD COLUMN IF NOT EXISTS usdt_volume DECIMAL(24, 8),
                ADD COLUMN IF NOT EXISTS btc_volume DECIMAL(24, 8)
            `);
        }
    } catch (error) {
        log(`Error checking table structure: ${error.message}`, 'ERROR');
        throw error;
    } finally {
        client.release();
    }
};
const fetchWithRetry = async (url, params = {}, attempt = 1) => {
    try {
        log(`Attempt ${attempt}: ${url}`, 'DEBUG');
        const response = await nownodesAxios.get(url, { params });

        // Проверяем, не вернул ли API ошибку в данных
        if (response.data?.error) {
            throw new Error(response.data.error);
        }

        await new Promise(resolve => setTimeout(resolve, config.api.delay));
        log(`Request successful (attempt ${attempt}): ${url}`, 'DEBUG');
        return response.data;
    } catch (error) {
        log(`Attempt ${attempt} failed: ${error.message}`, 'WARN');
        if (attempt >= config.api.maxRetries) {
            throw new Error(`Request failed after ${attempt} attempts: ${error.message}`);
        }
        const delay = Math.pow(2, attempt) * 1000;
        await new Promise(resolve => setTimeout(resolve, delay));
        return fetchWithRetry(url, params, attempt + 1);
    }
};

const fetchFallbackPrices = async (date) => {
    try {
        // Попробуем получить цены из базы данных
        const client = await pool.connect();
        try {
            const res = await client.query(
                `SELECT price_usdt, price_btc 
                 FROM token_prices 
                 WHERE token_symbol = $1 
                 ORDER BY timestamp DESC 
                 LIMIT 1`,
                [config.blockchain.symbol]
            );

            if (res.rows.length > 0) {
                return {
                    usd: parseFloat(res.rows[0].price_usdt) || 100,
                    btc: parseFloat(res.rows[0].price_btc) || 0.002
                };
            }
        } finally {
            client.release();
        }

        // Если в базе нет данных, используем фиксированные значения
        return {
            usd: 100,    // Примерное значение USD
            btc: 0.002   // Примерное значение BTC
        };
    } catch (error) {
        log(`Error fetching fallback prices: ${error.message}`, 'WARN');
        return {
            usd: 100,
            btc: 0.002
        };
    }
};
const getBlockchainInfo = async () => {
    try {
        // Используем nownodesAxios.get вместо fetchWithRetry
        const response = await nownodesAxios.get('/status');
        const data = response.data;
        return {
            height: data.blockbook?.bestHeight || 0,
            lastBlockTime: data.blockbook?.lastBlockTime
                ? DateTime.fromISO(data.blockbook.lastBlockTime)
                : DateTime.now()
        };
    } catch (error) {
        log(`Failed to get blockchain info: ${error.message}`, 'ERROR');
        throw error;
    }
};

const blockCache = new Map();
const getBlockByNumber = async (blockNumber) => {
    if (blockCache.has(blockNumber)) {
        return blockCache.get(blockNumber);
    }

    try {
        await new Promise(resolve => setTimeout(resolve, config.api.delay));
        const response = await nownodesAxios.get(`/block/${blockNumber}`);
        const data = response.data;

        const result = {
            height: data.height,
            time: data.time,
            timestamp: data.time ? DateTime.fromSeconds(data.time) : null
        };

        blockCache.set(blockNumber, result);
        return result;
    } catch (error) {
        // ... обработка ошибок
    }
};
const findBlockByTime = async (targetTime, lowBlock, highBlock) => {
    let closestBlock = { height: lowBlock, timestamp: DateTime.fromSeconds(0) };
    let MAX_ITERATIONS = 20;

    while (lowBlock <= highBlock && MAX_ITERATIONS-- > 0) {
        const midBlock = Math.floor((lowBlock + highBlock) / 2);
        const block = await getBlockByNumber(midBlock);

        if (!block || !block.timestamp) {
            log(`Block ${midBlock} not found or has no timestamp`, 'WARN');
            break;
        }

        log(`Checking block ${midBlock} (${block.timestamp.toISO()}) vs target ${targetTime.toISO()}`, 'DEBUG');

        if (block.timestamp < targetTime) {
            closestBlock = { height: midBlock, timestamp: block.timestamp };
            lowBlock = midBlock + 1;
        } else {
            highBlock = midBlock - 1;
        }
    }

    // Добавляем небольшой запас (+10 блоков), чтобы гарантированно попасть в нужный диапазон
    closestBlock.height = Math.min(closestBlock.height + 10, highBlock);

    log(`Found block ${closestBlock.height} with time ${closestBlock.timestamp.toISO()} for target ${targetTime.toISO()}`, 'INFO');
    return closestBlock;
};

const getBlockRangeOptimized = async (currentHeight) => {
    try {
        const configStart = DateTime.fromISO(config.processing.dateRange.startDate, { zone: 'utc' });
        const configEnd = DateTime.fromISO(config.processing.dateRange.endDate, { zone: 'utc' }).endOf('day');

        // Получаем первый блок после указанной даты
        const startBlock = await findBlockByTime(configStart, 0, currentHeight);

        // Получаем последний блок до указанной даты
        const endBlock = await findBlockByTime(configEnd, startBlock.height, currentHeight);

        return {
            fromBlock: startBlock.height,
            toBlock: endBlock.height,
            startTimestamp: Math.floor(configStart.toSeconds()),
            endTimestamp: Math.floor(configEnd.toSeconds())
        };
    } catch (error) {
        log(`Error calculating block range: ${error.message}`, 'ERROR');
        return {
            fromBlock: 0,
            toBlock: currentHeight,
            startTimestamp: Math.floor(CONFIG_START.toSeconds()),
            endTimestamp: Math.floor(CONFIG_END.toSeconds())
        };
    }
};

const getPriceForTimestamp = async (timestamp) => {
    if (!isInConfigRange(timestamp)) {
        log(`Timestamp ${timestamp} outside config range`, 'WARN');
        return { price_usdt: 0, price_btc: 0 };
    }

    const txDate = DateTime.fromSeconds(timestamp, { zone: 'utc' });
    const utcHour = txDate.toUTC().startOf('hour').toISO();

    const client = await pool.connect();
    try {
        // Сначала ищем точное совпадение по часу
        let res = await client.query(
            `SELECT price_usdt, price_btc 
             FROM token_prices 
             WHERE token_symbol = $1 AND timestamp = $2`,
            [config.blockchain.symbol, utcHour]
        );

        // Если нет, ищем ближайшие предыдущие данные
        if (res.rows.length === 0) {
            res = await client.query(
                `SELECT price_usdt, price_btc 
                 FROM token_prices 
                 WHERE token_symbol = $1 AND timestamp <= $2
                 ORDER BY timestamp DESC 
                 LIMIT 1`,
                [config.blockchain.symbol, utcHour]
            );
        }

        // Если все еще нет, берем последние доступные данные
        if (res.rows.length === 0) {
            res = await client.query(
                `SELECT price_usdt, price_btc 
                 FROM token_prices 
                 WHERE token_symbol = $1 
                 ORDER BY timestamp DESC 
                 LIMIT 1`,
                [config.blockchain.symbol]
            );
        }

        // Если вообще нет данных, используем fallback
        if (res.rows.length === 0) {
            log(`No price data found for ${utcHour}, using fallback`, 'WARN');
            const fallback = await fetchFallbackPrices(DateTime.fromSeconds(timestamp));
            return {
                price_usdt: fallback.usd,
                price_btc: fallback.btc
            };
        }

        return res.rows[0];
    } finally {
        client.release();
    }
};

const getAddressTransactions = async (address, fromBlock, toBlock) => {
    try {
        let allTransactions = [];
        let page = 1;
        const pageSize = 1000; // Максимальный размер страницы, поддерживаемый API
        let hasMore = true;

        while (hasMore) {
            const response = await fetchWithRetry(`/address/${address}`, {
                details: 'txs',
                pageSize: pageSize,
                from: fromBlock,
                to: toBlock,
                page: page
            });

            if (!response?.transactions || response.transactions.length === 0) {
                hasMore = false;
                break;
            }

            // Добавим логирование для отладки
            log(`Found ${response.transactions.length} transactions on page ${page} for address ${address}`, 'DEBUG');

            // Фильтруем транзакции по дате
            const filteredTxs = response.transactions.filter(tx => {
                const txTime = tx.blockTime || tx.time;
                if (!txTime) {
                    log(`Transaction ${tx.txid} has no timestamp`, 'DEBUG');
                    return false;
                }

                const isInRange = isInConfigRange(txTime);
                if (!isInRange) {
                    log(`Transaction ${tx.txid} with time ${new Date(txTime * 1000).toISOString()} outside range`, 'DEBUG');
                }
                return isInRange;
            });

            allTransactions = allTransactions.concat(filteredTxs);

            // Если получено меньше транзакций, чем pageSize, значит это последняя страница
            if (response.transactions.length < pageSize) {
                hasMore = false;
            } else {
                page++;
                // Добавляем небольшую задержку между страницами
                await new Promise(resolve => setTimeout(resolve, config.api.delay));
            }
        }

        log(`Total filtered transactions for ${address}: ${allTransactions.length}`, 'INFO');
        return allTransactions;
    } catch (error) {
        log(`Failed to get transactions for ${address}: ${error.message}`, 'ERROR');
        return [];
    }
};

// Определение направления транзакции
const getTransactionDirection = (tx, address) => {
    const isSender = tx.vin?.some(input => input.addresses?.includes(address));
    const isReceiver = tx.vout?.some(output => output.addresses?.includes(address));

    if (isSender && isReceiver) {
        const sentAmount = tx.vin
            .filter(input => input.addresses?.includes(address))
            .reduce((sum, input) => sum + parseFloat(input.value || 0), 0);
        const receivedAmount = tx.vout
            .filter(output => output.addresses?.includes(address))
            .reduce((sum, output) => sum + parseFloat(output.value || 0), 0);
        return receivedAmount < sentAmount ? 'outgoing' : 'internal';
    }
    if (isSender) return 'outgoing';
    return 'incoming';
};

// Расчет суммы транзакции
const calculateTransactionAmount = (tx, address) => {
    const direction = getTransactionDirection(tx, address);
    if (direction === 'incoming' || direction === 'internal') {
        return (tx.vout || [])
            .filter(output => output.addresses?.includes(address))
            .reduce((sum, output) => sum + parseFloat(output.value || 0), 0);
    }
    if (direction === 'outgoing') {
        const sentAmount = (tx.vin || [])
            .filter(input => input.addresses?.includes(address))
            .reduce((sum, input) => sum + parseFloat(input.value || 0), 0);
        const changeAmount = (tx.vout || [])
            .filter(output => output.addresses?.includes(address))
            .reduce((sum, output) => sum + parseFloat(output.value || 0), 0);
        return sentAmount - changeAmount;
    }
    return 0;
};

// Проверка на подозрительность
const checkIfSuspicious = (tx) => {
    const amount = parseFloat(tx.value) || 0;
    return satoshiToBtc(amount) > 5000 || (tx.vout?.length || 0) > 10;
};

// Сохранение батча транзакций
const satoshiToBtc = (satoshi) => satoshi / 100000000;

const litoshisToLtc = (litoshis) => litoshis / 100000000;

// Модифицированная функция сохранения транзакций
const saveTransactionsBatch = async (transactions, blockchainId, client) => {
    try {
        await client.query('BEGIN');
        let successCount = 0;

        for (const tx of transactions) {
            try {
                const txTimestamp = tx.blockTime || tx.time;
                if (!txTimestamp || !isInConfigRange(txTimestamp)) continue;

                // Получаем цены
                const { price_usdt, price_btc } = await getPriceForTimestamp(txTimestamp);
                const amountLtc = litoshisToLtc(calculateTransactionAmount(tx, tx.walletAddress));

                // Проверяем обязательные поля
                if (!tx.txid) {
                    log(`Transaction missing txid: ${JSON.stringify(tx)}`, 'ERROR');
                    continue;
                }

                await client.query(
                    `INSERT INTO transactions (
                        blockchain_id, tx_hash, timestamp, from_address, to_address,
                        direction, token_symbol, amount, gas_fee, tx_type,
                        is_suspicious, usdt_volume, btc_volume
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (tx_hash) DO NOTHING`,
                    [
                        blockchainId,
                        tx.txid,
                        DateTime.fromSeconds(txTimestamp).toISO(),
                        tx.vin?.[0]?.addresses?.[0] || 'unknown',
                        tx.vout?.[0]?.addresses?.[0] || 'unknown',
                        getTransactionDirection(tx, tx.walletAddress),
                        config.blockchain.symbol,
                        amountLtc,
                        litoshisToLtc(parseFloat(tx.fees) || 0),
                        'transfer',
                        checkIfSuspicious(tx),
                        amountLtc * (price_usdt || 0),
                        amountLtc * (price_btc || 0)
                    ]
                );
                successCount++;
            } catch (error) {
                log(`Error processing tx ${tx.txid}: ${error.message}`, 'ERROR');
                // Продолжаем обработку следующих транзакций
            }
        }

        await client.query('COMMIT');
        return successCount;
    } catch (error) {
        await client.query('ROLLBACK');
        log(`Batch failed: ${error.message}`, 'ERROR');
        return 0;
    }
};

// Обработка кошелька с батчами
const processWallet = async (wallet, blockchainId, blockRange) => {
    await checkTableStructure();
    const client = await pool.connect();
    let successCount = 0;

    try {
        log(`Starting processing wallet ${wallet.address}`, 'INFO');
        const transactions = await getAddressTransactions(
            wallet.address,
            blockRange.fromBlock,
            blockRange.toBlock
        );

        if (transactions.length === 0) {
            log(`No transactions for wallet ${wallet.address} in range`, 'DEBUG');
            return 0;
        }

        log(`Processing ${transactions.length} transactions for wallet ${wallet.address}`, 'INFO');

        // Обрабатываем небольшими батчами
        const batchSize = 50; // Можно увеличить, если сервер БД справляется
        for (let i = 0; i < transactions.length; i += batchSize) {
            const batch = transactions.slice(i, i + batchSize).map(tx => ({
                ...tx,
                walletAddress: wallet.address
            }));

            const batchSuccess = await saveTransactionsBatch(batch, blockchainId, client);
            successCount += batchSuccess;

            if (batchSuccess < batch.length) {
                log(`Batch ${i / batchSize + 1} had ${batch.length - batchSuccess} failures`, 'WARN');
            }

            // Добавляем небольшую задержку между батчами
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        return successCount;
    } catch (error) {
        log(`Error processing wallet ${wallet.address}: ${error.message}`, 'ERROR');
        return 0;
    } finally {
        client.release();
        processedTransactions += successCount;
        processedWallets++;
        log(`Finished processing wallet ${wallet.address}: ${successCount} transactions saved`, 'INFO');
    }
};

// Получение ID блокчейна
const getBlockchainId = async () => {
    const client = await pool.connect();
    try {
        const res = await client.query(
            'SELECT id FROM blockchains WHERE symbol = $1',
            [config.blockchain.symbol]
        );
        if (res.rows.length === 0) {
            throw new Error(`Blockchain ${config.blockchain.symbol} not found`);
        }
        return res.rows[0].id;
    } finally {
        client.release();
    }
};

// Получение кошельков
const getWalletsForBlockchain = async (blockchainId) => {
    const client = await pool.connect();
    try {
        const res = await client.query(
            'SELECT id, address FROM wallets WHERE blockchain_id = $1',
            [blockchainId]
        );
        return res.rows;
    } finally {
        client.release();
    }
};

// Основная функция
const processWallets = async () => {
    await checkDatabaseConnection();

    log(`STRICT DATE RANGE CONTROL: ${CONFIG_START.toISO()} to ${CONFIG_END.toISO()}`);

    const startTime = Date.now();
    log(`Starting processing at ${new Date(startTime).toISOString()}`);

    try {
        // 1. Сначала получаем информацию о блокчейне
        const chainInfo = await getBlockchainInfo();
        log(`Current blockchain height: ${chainInfo.height}`);

        // 2. Получаем диапазон блоков с обработкой ошибок
        let blockRange;
        try {
            blockRange = await getBlockRangeOptimized(chainInfo.height);
            log(`Block range: ${blockRange.fromBlock}-${blockRange.toBlock} (${DateTime.fromSeconds(blockRange.startTimestamp).toISO()} - ${DateTime.fromSeconds(blockRange.endTimestamp).toISO()})`);
        } catch (error) {
            log(`Using fallback block range due to error: ${error.message}`, 'WARN');
            blockRange = {
                fromBlock: 0,
                toBlock: chainInfo.height,
                startTimestamp: Math.floor(CONFIG_START.toSeconds()),
                endTimestamp: Math.floor(CONFIG_END.toSeconds())
            };
        }

        // 3. Проверяем покрытие цен
        const priceCoverage = await pool.query(`
            SELECT 
                MIN(timestamp) as first_price,
                MAX(timestamp) as last_price,
                COUNT(*) as total
            FROM token_prices
            WHERE token_symbol = $1`,
            [config.blockchain.symbol]
        );
        log(`Price coverage: ${priceCoverage.rows[0].first_price} to ${priceCoverage.rows[0].last_price} (${priceCoverage.rows[0].total} records)`);

        // 4. Сначала заполняем цены
        //await populateTokenPrices();

        // 5. Затем обрабатываем транзакции
        const blockchainId = await getBlockchainId();
        const wallets = await getWalletsForBlockchain(blockchainId);

        totalWallets = wallets.length;
        log(`Found ${totalWallets} wallets to process`);

        for (const wallet of wallets) {
            await processWallet(wallet, blockchainId, blockRange);
        }

        const duration = ((Date.now() - startTime) / 1000).toFixed(2);
        log(`Processing completed in ${duration} seconds. Processed: ${processedWallets}/${totalWallets} wallets, ${processedTransactions} transactions`);
    } catch (error) {
        log(`Critical error: ${error.message}`, 'ERROR');
        console.error(error.stack);
    } finally {
        await pool.end();
    }
};

// Запуск
(async () => {
    try {
        await processWallets();
        process.exit(0);
    } catch (error) {
        log(`Fatal error: ${error.message}`, 'ERROR');
        console.error(error.stack);
        process.exit(1);
    }
})();