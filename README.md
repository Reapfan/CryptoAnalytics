# Blockchain Monitoring Service

## Описание проекта

Сервис мониторинга активности адресов в блокчейне Litecoin (LTC) за период с 01.03.2025 по 31.03.2025. Проект собирает, анализирует и визуализирует данные о транзакциях, исключая подозрительные адреса, для выявления точек роста на основе аналитики объёмов.

## Структура проекта
```
/
├── code/ # Папка со скриптами
│ ├── aggregatedVolumes.js # Заполнение таблицы aggregatedVolumes
│ ├── tokenPrices.js # Заполнение таблицы tokenPrices
│ ├── tokenVolumes.js # Заполнение таблицы tokenVolumes
│ └── transactionsToDB.js # Заполнение таблицы transactionsToDB
│
├── dataBase/ # Папка с данными проекта
│ ├── agregated_volumes.csv # Заполненная таблица agregated_volumes
│ ├── blockchains.csv # Заполненная таблица blockchains
│ ├── internDB # Дамп заполненной базы данных
│ ├── tech_wallet.csv # Заполненная таблица tech_wallet
│ ├── token_prices.csv # Заполненная таблица token_prices
│ ├── token_volumes.csv # Заполненная таблица token_volumes
│ ├── transactions.csv # Заполненная таблица transactions
│ └── wallets.csv # Заполненная таблица wallets
│
├── README.md # Документация
└── internship.pdf # Аналитический отчёт
```

### Основные модули
1. **tokenPrices.js**  
   - Запрашивает исторические цены LTC через CoinGecko API.
   - Сохраняет часовые котировки в таблицу `token_prices`.
     
2. **transactionsToDB.js**  
   - Получает транзакции из блокчейна через NOWNodes API.
   - Автоматически помечает подозрительные транзакции (связанные с адресами из `tech_wallets` или объёмом >500k USDT).
   - Сохраняет данные в таблицу `transactions` с расчётом объёмов в USDT и BTC.

4. **tokenVolumes.js**  
   - Агрегирует объёмы транзакций по кошелькам, исключая подозрительные (is_suspicious = TRUE).
   - Заполняет таблицу `token_volumes` накопительными суммами.

5. **aggregatedVolumes.js**  
   - Формирует сводные данные по часам, игнорируя технические транзакции.
   - Сохраняет результаты в таблицу `aggregated_volumes`.

### База данных
**Таблицы, заполняемые программой:**
- `transactions`, `token_prices`, `token_volumes`, `aggregated_volumes`.

**Предоставленные таблицы (не заполняются скриптами):**
- `blockchains`, `wallets`, `tech_wallet` — содержат исходные данные:
  - Список блокчейнов и кошельков для мониторинга.
  - Технические адреса (например, биржевые кошельки), которые исключаются из анализа.

### Анализ подозрительных кошельков
- Транзакции с адресов из `tech_wallets` автоматически помечаются флагом `is_suspicious`.
- Объёмы таких транзакций не учитываются в агрегированных отчётах.
- Крупные операции (>500k USDT) логируются для ручной проверки.

### Отчёт
На основе "чистых" данных (без учёта tech_wallets) подготовлен аналитический отчёт (PDF), включающий:
- Доли конкурентов в объёмах USDT/BTC.
- Динамику изменения долей по неделям.
- Средние объёмы транзакций и рекомендации для обменников.

## Настройка
1. Установите PostgreSQL и импортируйте дамп БД (включая предзаполненные таблицы `blockchains`, `wallets`, `tech_wallet`).
2. Настройте подключение в файлах `.js` (хост, порт, учётные данные).
3. Добавьте API-ключи:
   - NOWNodes в `transactionsToDB.js`.
   - CoinGecko в `tokenPrices.js`.

## Запуск
Выполните модули в порядке:
```bash
node tokenPrices.js
node transactionsToDB.js
node tokenVolumes.js
node aggregatedVolumes.js
