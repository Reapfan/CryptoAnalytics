const { Pool } = require('pg'); // или другой драйвер для вашей БД

// Настройки подключения к БД
const pool = new Pool({
    user: 'your_user',
    host: 'your_host',
    database: 'your_db',
    password: 'your_password',
    port: 5432
});

async function fillAggregatedVolumes() {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    // 1. Получаем все кошельки
    const wallets = await client.query('SELECT id FROM wallets WHERE blockchain_id = 2');

    // 2. Устанавливаем временной диапазон (МСК)
    const startTime = new Date('2025-03-01T00:00:00+03:00');
    const endTime = new Date('2025-04-01T00:00:00+03:00');
    const hourStep = 60 * 60 * 1000; // 1 час

    console.log(`Диапазон: ${startTime.toISOString()} - ${endTime.toISOString()}`);

    for (const wallet of wallets.rows) {
      console.log(`Кошелек ${wallet.id}`);

      // 3. Перебираем часы в диапазоне
      for (let iterTime = startTime; iterTime <= endTime; iterTime = new Date(iterTime.getTime() + hourStep)) {

        // 4. Находим последние данные ≤ текущему часу
        const lastData = await client.query(`
          SELECT 
            COALESCE(MAX(total_tk_outgoing_usdt), 0) as usdt_sum,
            COALESCE(MAX(total_tk_outgoing_btc), 0) as btc_sum
          FROM token_volumes
          WHERE wallet_id = $1 AND date_volume <= $2 AND date_volume < ($2 + interval '1 hour')
          GROUP BY wallet_id
        `, [wallet.id, iterTime]);

        const sums = lastData.rows[0] || { usdt_sum: 0, btc_sum: 0 };

        // 5. Записываем данные (время в UTC)
        await client.query(`
          INSERT INTO aggregated_volumes 
          (wallet_id, date_volume, total_outgoing_usdt, total_outgoing_btc)
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (wallet_id, date_volume) 
          DO UPDATE SET
            total_outgoing_usdt = EXCLUDED.total_outgoing_usdt,
            total_outgoing_btc = EXCLUDED.total_outgoing_btc
        `, [
          wallet.id,
          iterTime,
          parseFloat(sums.usdt_sum) || 0,
          parseFloat(sums.btc_sum) || 0
        ]);
      }
    }

    await client.query('COMMIT');
    console.log('✅ Данные успешно добавлены');
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('❌ Ошибка:', error);
  } finally {
    client.release();
    pool.end();
  }
}

fillAggregatedVolumes();
