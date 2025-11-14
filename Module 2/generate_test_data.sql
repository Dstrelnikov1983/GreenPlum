-- ============================================
-- Скрипт генерации тестовых данных
-- Модуль 2: Работа с данными в GreenPlum
-- ============================================

-- Создание схемы для тестовых данных
CREATE SCHEMA IF NOT EXISTS test_data;
SET search_path TO test_data, public;

-- ============================================
-- 1. Справочник категорий продуктов
-- ============================================

CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER,
    description TEXT
) DISTRIBUTED REPLICATED;

INSERT INTO product_categories (category_name, parent_category_id, description) VALUES
('Electronics', NULL, 'Electronic devices and accessories'),
('Computers', 1, 'Desktop and laptop computers'),
('Smartphones', 1, 'Mobile phones and accessories'),
('Clothing', NULL, 'Apparel and fashion'),
('Men Clothing', 4, 'Clothing for men'),
('Women Clothing', 4, 'Clothing for women'),
('Food & Beverages', NULL, 'Food and drink products'),
('Books', NULL, 'Books and publications'),
('Home & Garden', NULL, 'Home improvement and garden supplies'),
('Toys & Games', NULL, 'Toys and entertainment');

-- ============================================
-- 2. Справочник продуктов
-- ============================================

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category_id INTEGER REFERENCES product_categories(category_id),
    brand VARCHAR(100),
    unit_cost DECIMAL(10, 2) NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    in_stock BOOLEAN DEFAULT true,
    stock_quantity INTEGER DEFAULT 0
) DISTRIBUTED REPLICATED;

-- Генерация 1000 продуктов
INSERT INTO products (product_name, category_id, brand, unit_cost, unit_price, in_stock, stock_quantity)
SELECT 
    CASE 
        WHEN cat_id IN (2, 3) THEN array['Laptop', 'Desktop', 'Smartphone', 'Tablet', 'Monitor', 'Keyboard'][1 + floor(random() * 6)::int] || ' Model ' || gs
        WHEN cat_id IN (5, 6) THEN array['T-Shirt', 'Jeans', 'Jacket', 'Dress', 'Shoes', 'Accessories'][1 + floor(random() * 6)::int] || ' Style ' || gs
        WHEN cat_id = 7 THEN array['Coffee', 'Tea', 'Juice', 'Snacks', 'Cookies', 'Bread'][1 + floor(random() * 6)::int] || ' ' || gs
        WHEN cat_id = 8 THEN array['Fiction', 'Non-Fiction', 'Science', 'History', 'Biography', 'Children'][1 + floor(random() * 6)::int] || ' Book ' || gs
        ELSE 'Product ' || gs
    END as product_name,
    cat_id as category_id,
    'Brand ' || (1 + floor(random() * 20)::int) as brand,
    (random() * 500 + 10)::decimal(10,2) as unit_cost,
    (random() * 1000 + 50)::decimal(10,2) as unit_price,
    random() > 0.1 as in_stock,
    (random() * 1000)::int as stock_quantity
FROM 
    generate_series(1, 1000) gs,
    (SELECT category_id as cat_id FROM product_categories WHERE parent_category_id IS NOT NULL) cats
LIMIT 1000;

-- ============================================
-- 3. Справочник клиентов
-- ============================================

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    registration_date DATE NOT NULL,
    customer_segment VARCHAR(50),
    loyalty_level VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20)
) DISTRIBUTED REPLICATED;

-- Генерация 10,000 клиентов
INSERT INTO customers (
    customer_name, email, phone, registration_date, 
    customer_segment, loyalty_level, city, country, postal_code
)
SELECT 
    array['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emma', 'Robert', 'Lisa'][1 + floor(random() * 8)::int] || ' ' ||
    array['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis'][1 + floor(random() * 8)::int] as customer_name,
    'customer' || gs || '@example.com' as email,
    '+1-' || (100 + floor(random() * 900)::int) || '-' || (100 + floor(random() * 900)::int) || '-' || (1000 + floor(random() * 9000)::int) as phone,
    CURRENT_DATE - (random() * 1095)::int as registration_date,
    CASE floor(random() * 4)::int
        WHEN 0 THEN 'Individual'
        WHEN 1 THEN 'Small Business'
        WHEN 2 THEN 'Enterprise'
        ELSE 'Government'
    END as customer_segment,
    CASE floor(random() * 4)::int
        WHEN 0 THEN 'Bronze'
        WHEN 1 THEN 'Silver'
        WHEN 2 THEN 'Gold'
        ELSE 'Platinum'
    END as loyalty_level,
    array['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 
          'San Antonio', 'San Diego', 'Dallas', 'San Jose'][1 + floor(random() * 10)::int] as city,
    CASE floor(random() * 5)::int
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'Canada'
        WHEN 2 THEN 'UK'
        WHEN 3 THEN 'Germany'
        ELSE 'France'
    END as country,
    (10000 + floor(random() * 90000)::int)::text as postal_code
FROM generate_series(1, 10000) gs;

-- ============================================
-- 4. Таблица заказов (партиционированная)
-- ============================================

CREATE TABLE orders (
    order_id BIGSERIAL,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    order_time TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    shipping_city VARCHAR(100),
    shipping_country VARCHAR(100),
    PRIMARY KEY (order_id, order_date)
)
DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2023-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION outliers
);

-- ============================================
-- 5. Детали заказов
-- ============================================

CREATE TABLE order_items (
    item_id BIGSERIAL,
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_percentage DECIMAL(5, 2) DEFAULT 0,
    subtotal DECIMAL(12, 2) NOT NULL,
    PRIMARY KEY (item_id, order_date)
)
DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2023-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION outliers
);

-- ============================================
-- 6. Функция для генерации заказов
-- ============================================

CREATE OR REPLACE FUNCTION generate_orders(num_orders INTEGER)
RETURNS void AS $$
DECLARE
    v_order_id BIGINT;
    v_order_date DATE;
    v_num_items INTEGER;
    v_customer_id INTEGER;
    v_total DECIMAL(12, 2);
BEGIN
    FOR i IN 1..num_orders LOOP
        -- Случайная дата за последние 2 года
        v_order_date := CURRENT_DATE - (random() * 730)::int;
        
        -- Случайный клиент
        v_customer_id := 1 + floor(random() * 10000)::int;
        
        -- Создание заказа
        INSERT INTO orders (
            customer_id, order_date, order_time, status, 
            total_amount, payment_method, shipping_address, 
            shipping_city, shipping_country
        ) VALUES (
            v_customer_id,
            v_order_date,
            v_order_date + (random() * 86400 || ' seconds')::interval,
            CASE floor(random() * 5)::int
                WHEN 0 THEN 'pending'
                WHEN 1 THEN 'processing'
                WHEN 2 THEN 'shipped'
                WHEN 3 THEN 'delivered'
                ELSE 'cancelled'
            END,
            0, -- будет обновлено
            CASE floor(random() * 4)::int
                WHEN 0 THEN 'Credit Card'
                WHEN 1 THEN 'PayPal'
                WHEN 2 THEN 'Bank Transfer'
                ELSE 'Cash on Delivery'
            END,
            (SELECT 'Address ' || customer_id FROM customers WHERE customer_id = v_customer_id),
            (SELECT city FROM customers WHERE customer_id = v_customer_id),
            (SELECT country FROM customers WHERE customer_id = v_customer_id)
        ) RETURNING order_id INTO v_order_id;
        
        -- Количество позиций в заказе (1-5)
        v_num_items := 1 + floor(random() * 5)::int;
        v_total := 0;
        
        -- Создание позиций заказа
        FOR j IN 1..v_num_items LOOP
            DECLARE
                v_product_id INTEGER;
                v_quantity INTEGER;
                v_unit_price DECIMAL(10, 2);
                v_discount DECIMAL(5, 2);
                v_subtotal DECIMAL(12, 2);
            BEGIN
                v_product_id := 1 + floor(random() * 1000)::int;
                v_quantity := 1 + floor(random() * 5)::int;
                v_unit_price := (SELECT unit_price FROM products WHERE product_id = v_product_id);
                v_discount := CASE WHEN random() > 0.7 THEN (random() * 20)::decimal(5,2) ELSE 0 END;
                v_subtotal := v_quantity * v_unit_price * (1 - v_discount / 100);
                
                INSERT INTO order_items (
                    order_id, order_date, product_id, quantity, 
                    unit_price, discount_percentage, subtotal
                ) VALUES (
                    v_order_id, v_order_date, v_product_id, v_quantity,
                    v_unit_price, v_discount, v_subtotal
                );
                
                v_total := v_total + v_subtotal;
            END;
        END LOOP;
        
        -- Обновление total_amount в заказе
        UPDATE orders SET total_amount = v_total 
        WHERE order_id = v_order_id;
        
        -- Прогресс каждые 10000 заказов
        IF i % 10000 = 0 THEN
            RAISE NOTICE 'Generated % orders...', i;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Successfully generated % orders', num_orders;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 7. Генерация данных
-- ============================================

-- Генерируем 100,000 заказов (займет несколько минут)
-- SELECT generate_orders(100000);

-- Для быстрого тестирования - 10,000 заказов
SELECT generate_orders(10000);

-- ============================================
-- 8. Обновление статистики
-- ============================================

ANALYZE product_categories;
ANALYZE products;
ANALYZE customers;
ANALYZE orders;
ANALYZE order_items;

-- ============================================
-- 9. Создание полезных представлений
-- ============================================

-- Представление: детализация заказов с информацией о клиентах и продуктах
CREATE VIEW order_details_view AS
SELECT 
    o.order_id,
    o.order_date,
    o.status,
    c.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.customer_segment,
    c.loyalty_level,
    oi.product_id,
    p.product_name,
    p.category_id,
    pc.category_name,
    oi.quantity,
    oi.unit_price,
    oi.discount_percentage,
    oi.subtotal,
    o.total_amount as order_total
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
JOIN product_categories pc ON p.category_id = pc.category_id;

-- Представление: агрегированная статистика по клиентам
CREATE VIEW customer_statistics AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.country,
    c.customer_segment,
    c.loyalty_level,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as total_spent,
    AVG(o.total_amount) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    MAX(o.order_date) - MIN(o.order_date) as customer_lifetime_days
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.country, 
    c.customer_segment, c.loyalty_level;

-- Представление: статистика по продуктам
CREATE VIEW product_statistics AS
SELECT 
    p.product_id,
    p.product_name,
    p.category_id,
    pc.category_name,
    p.brand,
    COUNT(oi.item_id) as times_ordered,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.subtotal) as total_revenue,
    AVG(oi.unit_price) as avg_selling_price
FROM products p
JOIN product_categories pc ON p.category_id = pc.category_id
LEFT JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY 
    p.product_id, p.product_name, p.category_id, 
    pc.category_name, p.brand;

-- ============================================
-- 10. Примеры запросов для проверки данных
-- ============================================

-- Общая статистика
SELECT 
    'Products' as entity,
    COUNT(*) as count
FROM products
UNION ALL
SELECT 'Customers', COUNT(*) FROM customers
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order Items', COUNT(*) FROM order_items;

-- Распределение заказов по месяцам
SELECT 
    DATE_TRUNC('month', order_date) as month,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;

-- Топ-10 продуктов по выручке
SELECT 
    p.product_name,
    pc.category_name,
    SUM(oi.subtotal) as revenue,
    SUM(oi.quantity) as quantity_sold
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN product_categories pc ON p.category_id = pc.category_id
GROUP BY p.product_name, pc.category_name
ORDER BY revenue DESC
LIMIT 10;

-- Топ-10 клиентов
SELECT 
    c.customer_name,
    c.country,
    c.loyalty_level,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name, c.country, c.loyalty_level
ORDER BY total_spent DESC
LIMIT 10;

RAISE NOTICE 'Test data generation completed successfully!';
