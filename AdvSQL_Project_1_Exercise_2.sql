AdvSQL_Project_1_Exercise_2.sql

-- EXERCISE 2

--APPROACH
    /* 1) copy/paste bits from Exercise 1 to isolate eligible customers
       2) filter out inactive customers
       3) find preferences by joining tag_id from customer_survey to recipe tags return top 3 in alpha asc
       4) pivot top 3 into columns
       5) CTE for recipes that flattens tags
       6) match recipe with food pref #1
       7) order by email
    
     */


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

, top_3_food_pref AS (
    
    SELECT
        cs.customer_id,
        ec.email,
        ec.first_name,
        trim(rt.tag_property) AS customer_tag,
        row_number() OVER (PARTITION BY cs.customer_id ORDER BY tag_property) AS tag_rank
    
    FROM vk_data.customers.customer_survey AS cs
    INNER JOIN eligible_customers AS ec
        ON cs.customer_id = ec.customer_id
    INNER JOIN vk_data.resources.recipe_tags AS rt
        ON cs.tag_id = rt.tag_id
    WHERE 
        is_active = TRUE /* customers who completed survey */
    QUALIFY tag_rank BETWEEN 1 AND 3 /*filter out customers who do not have a food preference but no more than 3 */

)

-- SELECT * FROM top_3_food_pref LIMIT 100 /*QA CTE*/

, pivot_top3_by_customer AS (

    SELECT
        *
    FROM top_3_food_pref
    PIVOT(
        max(customer_tag) for tag_rank IN (1,2,3)) /* need to get one record per customer by flipping tags from rows to columns */
        AS pivot_values(
            customer_id,
            email,
            first_name,
            food_pref_1,
            food_pref_2,
            food_pref_3
        )
    
)

-- SELECT * FROM pivot_top3_by_customer LIMIT 50 /*QA CTE*/
 
, recipes_by_tag AS (

    SELECT
        recipe_id,
        recipe_name,
        trim(replace(flat_tag.value,'"','')) AS recipe_tag /* clean data, remove whitespace, remove quotations around value */
    FROM vk_data.chefs.recipe
    , table(flatten(tag_list)) AS flat_tag
)

-- SELECT * FROM recipes_by_tag limit 100 /*QA CTE*/

, suggested_recipe AS (
    
    SELECT
        customer_id,
        any_value(recipe_name) AS suggested_recipe
    FROM top_3_food_pref AS tt
    INNER JOIN recipes_by_tag AS rt
        ON tt.customer_tag = rt.recipe_tag AND tt.tag_rank = 1 /* map food_pref_1 tag to recipe tag */
    GROUP BY 1

)

    -- SELECT * FROM suggested_recipe /*QA CTE*/
, final AS (
    
    SELECT
        p.customer_id,
        email,
        first_name,
        food_pref_1,
        food_pref_2,
        food_pref_3,
        suggested_recipe
       
    FROM pivot_top3_by_customer as p
    LEFT JOIN suggested_recipe AS sr 
        ON p.customer_id = sr.customer_id
    ORDER BY email
    
)

SELECT * FROM final
    
    
