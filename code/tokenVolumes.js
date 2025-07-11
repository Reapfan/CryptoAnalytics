const { Pool } = require('pg'); // или другой драйвер для вашей БД

// Настройки подключения к БД
const pool = new Pool({
    user: 'your_user',
    host: 'your_host',
    database: 'your_db',
    password: 'your_password',
    port: 5432
});

async function fillTokenVolumes() {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Получаем все кошельки с blockchain_id = 2
    const walletsQuery = 'SELECT id, address FROM wallets WHERE blockchain_id = 2';
    const walletsRes = await client.query(walletsQuery);
    const wallets = walletsRes.rows;

    for (const wallet of wallets) {
      // 2. Получаем транзакции для текущего кошелька
      const transactionsQuery = `
        SELECT timestamp, usdt_volume, btc_volume 
        FROM transactions 
        WHERE from_address = $1 AND is_suspicious = false
        ORDER BY timestamp ASC
      `;
      const transactionsRes = await client.query(transactionsQuery, [wallet.address]);
      const transactions = transactionsRes.rows;

      // 3. Инициализируем суммы
      let usdtSum = 0;
      let btcSum = 0;

      for (const tx of transactions) {
        // 5. Прибавляем объемы к суммам
        usdtSum += parseFloat(tx.usdt_volume) || 0;
        btcSum += parseFloat(tx.btc_volume) || 0;

        // 6. Используем UPSERT для вставки или обновления
        const upsertQuery = `
          INSERT INTO token_volumes 
          (wallet_id, date_volume, total_tk_outgoing_usdt, total_tk_outgoing_btc, token_symbol, contract_address)
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (wallet_id, token_symbol, date_volume) 
          DO UPDATE SET
            total_tk_outgoing_usdt = EXCLUDED.total_tk_outgoing_usdt,
            total_tk_outgoing_btc = EXCLUDED.total_tk_outgoing_btc,
            contract_address = EXCLUDED.contract_address
        `;
        await client.query(upsertQuery, [
          wallet.id,
          tx.timestamp,
          usdtSum,
          btcSum,
          'ltc',
          null
        ]);
      }
    }

    await client.query('COMMIT');
    console.log('Таблица token_volumes успешно заполнена');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Ошибка при заполнении token_volumes:', error);
  } finally {
    client.release();
  }
}

fillTokenVolumes()
  .then(() => pool.end())
  .catch(err => {
    console.error(err);
    pool.end();
  });
