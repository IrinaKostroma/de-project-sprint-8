# Сервис

![Service Architecture](service.png)

1. Пример входного сообщения об актуальных акциях ресторана
```
first_message: {
        "restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
        "adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003",
        "adv_campaign_content": "first campaign",
        "adv_campaign_owner": "Ivanov Ivan Ivanovich",
        "adv_campaign_owner_contact": "iiivanov@restaurant.ru",
        "adv_campaign_datetime_start": 1659203516,
        "adv_campaign_datetime_end": 2659207116,
        "datetime_created": 1659131516
}
```

2. Схема Postgres таблицы с информацией о подписках клиентов на определенные рестораны

```
-- DROP TABLE public.subscribers_restaurants;

CREATE TABLE public.subscribers_restaurants (
    id serial4 NOT NULL,
    client_id varchar NOT NULL,
    restaurant_id varchar NOT NULL,
    CONSTRAINT pk_id PRIMARY KEY (id)
);

-- Пример заполненных данных
id|client_id                           |restaurant_id                       |
--+------------------------------------+------------------------------------+
 1|223e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 2|323e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 3|423e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 4|523e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 5|623e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 6|723e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 7|823e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
 8|923e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174001|
 9|923e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174001|
10|023e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174000|
```

3. Схема выходной таблицы в Postgres для фидбека
```
    -- Выходная таблица
    -- DROP TABLE public.subscribers_feedback;
    
    CREATE TABLE public.subscribers_feedback (
      id serial4 NOT NULL,
        restaurant_id text NOT NULL,
        adv_campaign_id text NOT NULL,
        adv_campaign_content text NOT NULL,
        adv_campaign_owner text NOT NULL,
        adv_campaign_owner_contact text NOT NULL,
        adv_campaign_datetime_start int8 NOT NULL,
        adv_campaign_datetime_end int8 NOT NULL,
        client_id text NOT NULL,
        trigger_datetime_created int4 NOT NULL,
        feedback varchar NULL,
        CONSTRAINT id_pk PRIMARY KEY (id)
    );
    
    -- Пример заполненных данных
    id|restaurant_id                       |adv_campaign_id                     |adv_campaign_content|adv_campaign_owner   |adv_campaign_owner_contact|adv_campaign_datetime_start|adv_campaign_datetime_end|datetime_created|client_id                           |trigger_datetime_created|feedback|
    --+------------------------------------+------------------------------------+--------------------+---------------------+--------------------------+---------------------------+-------------------------+----------------+------------------------------------+------------------------+--------+
     1|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|223e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     2|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|323e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     3|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|423e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     4|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|523e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     5|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|623e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     6|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|723e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     7|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|823e4567-e89b-12d3-a456-426614174000|              1659304828|        |
     8|123e4567-e89b-12d3-a456-426614174000|123e4567-e89b-12d3-a456-426614174003|first campaign      |Ivanov Ivan Ivanovich|iiivanov@restaurant.ru    |                 1659203516|               2659207116|      1659131516|023e4567-e89b-12d3-a456-426614174000|              1659304828|        |
    
```
4. Push-сообщение в Kafka
```
{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000",
"adv_campaign_id":"123e4567-e89b-12d3-a456-426614174003",
"adv_campaign_content":"first campaign",
"adv_campaign_owner":"Ivanov Ivan Ivanovich",
"adv_campaign_owner_contact":"iiivanov@restaurant.ru",
"adv_campaign_datetime_start":1659203516,
"adv_campaign_datetime_end":2659207116,
"client_id":"023e4567-e89b-12d3-a456-426614174000",
"datetime_created":1659131516,
"trigger_datetime_created":1659304828}
```
