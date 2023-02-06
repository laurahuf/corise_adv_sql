-- EXERCISE 1


WITH customers AS(
    
    SELECT 
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email,
        ca.customer_city,
        ca.customer_state
    FROM customers.customer_data AS cd /*need to return requested data points (ie: customer_id, first_name,etc)*/
    INNER JOIN customers.customer_address AS ca  /*need customer city/state data*/
        ON cd.customer_id = ca.customer_id
)

-- SELECT * FROM customers LIMIT 50 /*QA CTE*/

,vk_locations AS (
    
    SELECT
        upper(trim(city_name)) AS city_name,
        upper(trim(state_abbr)) AS state_abbr,
        geo_location
    FROM vk_data.resources.us_cities AS us  /*list of locations eligible to order vk*/
    QUALIFY row_number() over (partition by city_name, state_abbr ORDER BY 1)=1 /*remove duplicate city || state pairs*/

)

-- SELECT * FROM vk_locations /*QA CTE*/

,eligible_customers AS (
    
    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.customer_city,
        c.customer_state AS customer_state_abbr,
        vkl.geo_location /*need to perform distance calculation*/
    FROM customers AS c
    INNER JOIN vk_locations AS vkl
        ON upper(trim(c.customer_city)) = upper(vkl.city_name) /*clean data, align formatting*/
        AND upper(trim(c.customer_state)) = upper(vkl.state_abbr) /*clean data, align formatting*/
)

-- SELECT * FROM eligible_customers LIMIT 50 /*QA CTE*/

,suppliers AS (
    
    SELECT
        s.supplier_id,
        s.supplier_name,
        s.supplier_city,
        s.supplier_state AS supplier_state_abbr,
        vkl.geo_location /*need to perform distance calculation*/
    FROM vk_data.suppliers.supplier_info AS s
    INNER JOIN vk_locations AS vkl
        ON upper(trim(s.supplier_city)) = upper(trim(vkl.city_name)) /*clean data, align formatting*/
        AND upper(trim(s.supplier_state)) = upper(trim(vkl.state_abbr)) /*clean data, align formatting*/
    
)

-- SELECT * FROM suppliers /*QA CTE*/

,closest_supplier_by_customer AS (
    
    SELECT
        ec.customer_id,
        ec.first_name,
        ec.last_name,
        ec.email,
        s.supplier_id,
        s.supplier_name,
        st_distance(ec.geo_location, s.geo_location)/1069 AS distance_from_supplier_to_customer_in_miles
        
    FROM eligible_customers AS ec
    CROSS JOIN suppliers AS s
        -- ON upper(trim(s.supplier_city)) = upper(trim(ec.customer_city)) /*clean data, align formatting*/
        -- AND upper(trim(s.supplier_state_abbr)) = upper(trim(ec.customer_state_abbr)) /*clean data, align formatting*/
    
    QUALIFY row_number() OVER (PARTITION BY customer_id order by distance_from_supplier_to_customer_in_miles) = 1 /*select smallest distance*/
    ORDER BY last_name, first_name
)

SELECT * FROM closest_supplier_by_customer limit 50
