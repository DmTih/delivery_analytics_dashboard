/*
Проект: Анализ пользовательской активности и эффективности работы курьеров

Цель: Анализ ключевых метрик сервиса для оценки нагрузки, выявления узких мест и повышения эффективности работы.

Описание используемых датасетов:
1. user_actions (действия пользователей)
- user_id — уникальный идентификатор пользователя
- order_id — идентификатор заказа
- action — тип действия: 'create_order' (создание заказа) или 'cancel_order' (отмена заказа)
- time — временная метка совершения действия

2. orders (информация о заказах)
- order_id — уникальный идентификатор заказа
- creation_time — дата и время создания заказа
- product_ids — массив идентификаторов товаров в заказе

3. courier_actions (действия курьеров)
- courier_id — уникальный идентификатор курьера
- order_id — идентификатор заказа
- action — тип действия: accept_order (принятие заказа) или deliver_order (доставка заказа)
- time — временная метка совершения действия

Что делал: 
1. Рассчитал текущее количество пользователей и курьеров
2. Проанализировал прирост пользователей и курьеров за период
3. Оценил динамику платящих пользователей и активных курьеров, рассчитал долю платящих
4. Определил количество платящих пользователей с более чем 1 заказом в день
5. Посчитал долю заказов от новых пользователей в общем числе заказов
6. Проанализировал нагрузку на курьеров (количество заказов на человека)
7. Рассчитал среднее время доставки заказа
8. Оценил почасовую нагрузку на сервис
9. Составил дашборд, сделал выводы, дал рекомендации 
*/

--ШАГ 1. Рассчитал текущее количество пользователей и курьеров

SELECT
    u.date,
    u.new_users,
    c.new_couriers,
    SUM(new_users) OVER(ORDER BY u.date)::INTEGER AS total_users,
    SUM(new_couriers) OVER(ORDER BY u.date)::INTEGER AS total_couriers
FROM
    (SELECT
        user_first_action AS date,
        COUNT(DISTINCT user_id) AS new_users 
     FROM
        (SELECT 
            user_id,
            MIN(time::DATE) AS user_first_action
         FROM user_actions
         GROUP BY user_id) sub_user_first_action
     GROUP BY date) u
JOIN 
    (SELECT
        coutier_first_action AS date,
        COUNT(DISTINCT courier_id) AS new_couriers 
     FROM
        (SELECT 
            courier_id,
            MIN(time::DATE) AS coutier_first_action
         FROM courier_actions
         GROUP BY courier_id) sub_coutier_first_action
     GROUP BY date) c
ON u.date = c.date
ORDER BY date


--ШАГ 2. Проанализировал прирост пользователей и курьеров за период

SELECT     
    date,
    new_users,
    new_couriers,
    total_users,
    total_couriers,
    ROUND((new_users::DECIMAL - LAG(new_users) OVER(ORDER BY date))/LAG(new_users) OVER(ORDER BY date)*100, 2) AS new_users_change,
    ROUND((new_couriers::DECIMAL - LAG(new_couriers) OVER(ORDER BY date))/LAG(new_couriers) OVER(ORDER BY date)*100, 2) AS new_couriers_change,
    ROUND((total_users::DECIMAL - LAG(total_users) OVER(ORDER BY date))/LAG(total_users) OVER(ORDER BY date)*100, 2) AS total_users_growth,
    ROUND((total_couriers::DECIMAL - LAG(total_couriers) OVER(ORDER BY date))/LAG(total_couriers) OVER(ORDER BY date)*100, 2) AS total_couriers_growth
FROM
    (SELECT
        u.date,
        u.new_users,
        c.new_couriers,
        SUM(new_users) OVER(ORDER BY u.date)::INTEGER AS total_users,
        SUM(new_couriers) OVER(ORDER BY u.date)::INTEGER AS total_couriers
     FROM   
        (SELECT
            user_first_action AS date,
            COUNT(DISTINCT user_id) AS new_users 
         FROM
            (SELECT 
                user_id,
                MIN(time::DATE) AS user_first_action
             FROM user_actions
             GROUP BY user_id) sub_user_first_action
         GROUP BY date) u
    JOIN 
        (SELECT
            coutier_first_action AS date,
            COUNT(DISTINCT courier_id) AS new_couriers 
         FROM
            (SELECT 
                courier_id,
                MIN(time::DATE) AS coutier_first_action
             FROM courier_actions
             GROUP BY courier_id) sub_coutier_first_action
         GROUP BY date) c
    ON u.date = c.date) AS sub_absolute_values
ORDER BY date


--ШАГ 3. Оценил динамику платящих пользователей и активных курьеров, рассчитал долю платящих

SELECT
  date,
  paying_users, --число платящих пользователей 
  active_couriers, --число активных курьеров 
  ROUND(paying_users::DECIMAL / total_users * 100, 2) AS paying_users_share, --доля платящих польз. в общем числе польз. на текущий день
  ROUND(active_couriers::DECIMAL / total_couriers * 100, 2) AS active_couriers_share -- доля активных курьеров в общем числе курьеров на тек.день
FROM 
(SELECT
  au.date,
  paying_users, --число платящих пользователей 
  active_couriers, --число активных курьеров 
  new_users, --число новых юзеров
  new_couriers, --число активных курьеров
  SUM(new_users) OVER(ORDER BY au.date)::INTEGER AS total_users, --общее число пользователей на текущий день
  SUM(new_couriers) OVER(ORDER BY au.date)::INTEGER AS total_couriers --общее число курьеров на текущий день
FROM 
-- Число платящих пользователей на дату, которые оформили хотя бы 1 заказ, который не был отменен
    (SELECT
      time::date AS date,
      COUNT(DISTINCT user_id) AS paying_users
    FROM user_actions
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') 
    GROUP BY date
    ) au
LEFT JOIN 
-- Число активных курьеров (если в данный день приняли хотя бы один заказ, который был доставлен
-- (возможно, уже на следующий день или позднее),или доставили любой заказ.)
    (SELECT 
      time::date AS date,
      COUNT(DISTINCT courier_id) AS active_couriers 
    FROM courier_actions
    WHERE (action = 'accept_order' 
        AND order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) 
        OR action = 'deliver_order'  
    GROUP BY date
    ) ac
ON au.date = ac.date
LEFT JOIN 
-- число новых пользователей на день
    (SELECT
        user_first_action AS date,
        COUNT(DISTINCT user_id) AS new_users 
     FROM
        (SELECT 
            user_id,
            MIN(time::DATE) AS user_first_action
         FROM user_actions
         GROUP BY user_id) sub_user_first_action
     GROUP BY date) nu
ON au.date = nu.date
LEFT JOIN 
-- число новых курьеров на день
    (SELECT
        coutier_first_action AS date,
        COUNT(DISTINCT courier_id) AS new_couriers 
     FROM
        (SELECT 
            courier_id,
            MIN(time::DATE) AS coutier_first_action
         FROM courier_actions
         GROUP BY courier_id) sub_coutier_first_action
     GROUP BY date) nс
ON au.date = nс.date
) AS final_sub
ORDER BY date


--ШАГ 4. Определил количество платящих пользователей с более чем 1 заказом в день

SELECT
    date,
    ROUND(SUM(single_order_users)::DECIMAL / COUNT(DISTINCT user_id) * 100, 2) AS single_order_users_share,
    ROUND(SUM(several_orders_users)::DECIMAL / COUNT(DISTINCT user_id) * 100, 2) AS several_orders_users_share
FROM (
    SELECT
        date,    
        user_id,
        COUNT(number_of_orders) FILTER (WHERE number_of_orders = 1) AS single_order_users,
        COUNT(number_of_orders) FILTER (WHERE number_of_orders > 1) AS several_orders_users
    FROM (
        SELECT
            time::date AS date,
            user_id,
            COUNT(order_id) AS number_of_orders
        FROM user_actions
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
        GROUP BY date, user_id
        ) AS sub_1
    GROUP BY date, user_id
    ) AS sub_2
GROUP BY date
ORDER BY date


--ШАГ 5. Посчитал долю заказов от новых пользователей в общем числе заказов

SELECT
    c.date, 
    orders, 
    first_orders, 
    new_users_orders, 
    ROUND(first_orders::DECIMAL /  orders * 100, 2) AS first_orders_share, 
    ROUND(new_users_orders::DECIMAL /  orders * 100, 2) AS new_users_orders_share
FROM (
    SELECT 
        time::DATE AS date,
        COUNT(DISTINCT order_id) AS orders --общее число заказов
    FROM user_actions
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') --не учитываю отмененные заказы
    GROUP BY date
    ) c
LEFT JOIN (
    SELECT
        date,
        COUNT(DISTINCT user_id) AS first_orders --число первых заказов (заказов, сделанных пользователями впервые).
    FROM (
        SELECT 
            user_id,
            MIN(time::DATE) AS date
        FROM user_actions 
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
        GROUP BY user_id 
        ) AS sub_1
    GROUP BY date
    ) d
ON c.date=d.date
LEFT JOIN (
    SELECT
        a.date,
        -- a.user_id,
        SUM(COALESCE(b.orders_per_new_users, 0))::INT AS new_users_orders --число заказов новых пользователей
    FROM (
        SELECT
            user_id,
            MIN(time::DATE) AS date --дата совершения первого действия пользователем
        FROM user_actions
        GROUP BY user_id
        ) a
    LEFT JOIN (
        SELECT 
            time::DATE AS date,
            user_id,
            COUNT(DISTINCT order_id) AS orders_per_new_users --число заказов на каждую дату для каждого пользователя
        FROM user_actions
        WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
        GROUP BY date, user_id
        ) b
    ON a.date = b.date AND a.user_id = b.user_id
    GROUP BY a.date
    ) e
ON d.date=e.date
ORDER BY date 


--ШАГ 6. Проанализировал нагрузку на курьеров (количество заказов на человека)

SELECT
    o.date,
    ROUND(paying_users::DECIMAL / active_couriers, 2) AS users_per_courier,
    ROUND(orders::DECIMAL / active_couriers, 2) AS orders_per_courier
FROM (
    -- Общее число заказов
    SELECT 
        creation_time::DATE AS date,
        COUNT(DISTINCT order_id) AS orders 
    FROM orders
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') --не учитываю отмененные заказы
    GROUP BY date
    ) o
LEFT JOIN (
    -- Число активных курьеров 
    SELECT 
        time::date AS date,
        COUNT(DISTINCT courier_id) AS active_couriers 
    FROM courier_actions
    WHERE (action = 'accept_order' 
        AND order_id IN (SELECT order_id FROM courier_actions WHERE action = 'deliver_order')) 
        OR action = 'deliver_order'  
    GROUP BY date
    ) c
ON o.date=c.date
LEFT JOIN (
    -- Число платящих пользователей на дату, которые оформили хотя бы 1 заказ, который не был отменен
    SELECT
        time::date AS date,
        COUNT(DISTINCT user_id) AS paying_users
    FROM user_actions
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') 
    GROUP BY date
    ) u
ON c.date=u.date
ORDER BY date


--ШАГ 7. Рассчитал среднее время доставки заказа

SELECT 
    date,
    ROUND(AVG(delta_min) FILTER (WHERE delta_min != 0))::INT  AS minutes_to_deliver
FROM (
    SELECT 
        time::DATE AS date,
        courier_id,
        order_id,
        action,
        EXTRACT(epoch FROM
            MAX(time) OVER(PARTITION BY order_id ORDER BY time) - 
            MIN(time) OVER(PARTITION BY order_id ORDER BY time)
            ) / 60 AS delta_min
    FROM courier_actions
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order') --отмененнные заказы 
        ) sub_1
GROUP BY date
ORDER BY date


--ШАГ 8. Оценил почасовую нагрузку на сервис

SELECT
    s.hour,
    s.successful_orders,
    c.canceled_orders,
    ROUND(c.canceled_orders::DECIMAL / (c.canceled_orders + s.successful_orders), 3) AS cancel_rate
FROM (
    SELECT
        DATE_PART('hour', creation_time)::INT AS hour,
        COUNT(order_id) AS successful_orders
    FROM orders
    WHERE order_id NOT IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
    GROUP BY hour
    ) s
JOIN (
    SELECT
        DATE_PART('hour', creation_time)::INT AS hour,
        COUNT(order_id) AS canceled_orders
    FROM orders
    WHERE order_id IN (SELECT order_id FROM user_actions WHERE action = 'cancel_order')
    GROUP BY hour
    ) c
ON s.hour = c.hour
ORDER BY hour

/*
Дашборд доступен по ссылке: https://redash.public.karpov.courses/public/dashboards/PMssnIEIAI2nG1giJmhyOFWA2hae0tNx4vzHPRaG?org_slug=default

Выводы:
1. Наблюдается устойчивый рост числа платящих пользователей и активных курьеров, что свидетельствует об успешном привлечении
   новых клиентов и эффективном использовании курьерских ресурсов.
2. В среднем 95% курьеров остаются активными, что указывает на оптимальную загрузку платформы. Единичное падение
   до 80% (6 сентября) коррелирует с общим снижением числа заказов и не является критичным.
3. Доля платящих клиентов упала с 97% до 18% — этот тревожный сигнал требует дополнительного анализа, так как может
   быть вызван либо притоком "холодных" пользователей, либо снижением retention существующих клиентов.  
4. Высокий уровень лояльности подтверждается тем, что 30% пользователей совершают более одного заказа в день, 
   при этом данные первого дня не являются репрезентативными из-за недостаточного периода наблюдения.
5. Положительная динамика абсолютных показателей (общее число заказов) сочетается со снижением относительных показателей 
   (доли первых заказов), что является нормальной ситуацией, так как рост всё больше обеспечивается повторными покупками постоянных клиентов.
6. Равномерная нагрузка на курьеров и стабильное среднее время доставки (20 минут) свидетельствуют, что текущее количество 
   курьеров оптимально и дополнительный набор не требуется.
7. Анализ почасовой нагрузки выявил пиковый период с 17:00 до 23:00, когда сервису требуется максимальная концентрация ресурсов.
8. Хотя абсолютное число отмен растёт вместе с увеличением заказов, доля отмен (cancel rate) остаётся стабильной, 
   при этом рост отмен в вечернее и ночное время требует отдельного изучения для оптимизации логистических процессов.

Рекомендации:
1. Сфокусироваться на улучшении удержания новых пользователей
2. Усилить операционные мощности в пиковые часы (17:00-23:00)
3. Оптимизировать процессы ночной доставки
4. Сохранять текущую стратегию работы с курьерами
*/

