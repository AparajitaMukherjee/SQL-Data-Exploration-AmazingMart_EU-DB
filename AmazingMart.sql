-- WORKING ON ADVENTURE WORKS
drop table datasets.amazingmart_salestarget;
rename table `amazingmart-listoforders` to `amazingmart_listoforders`;

alter table datasets.amazingmart_salestarget rename column `ï»¿Month of Order Date` to `Month_OrderDate`;

alter table datasets.amazingmart_orderbreakdown rename column `Sub-Category` to `Sub_Category`;

SET SQL_SAFE_UPDATES = 0;
update datasets.amazingmart_salestarget
set Target = replace(replace(replace(Target, "$",""), ".00",""),",","");
SET SQL_SAFE_UPDATES = 1;

alter table datasets.amazingmart_orderbreakdown modify Profit int;

show columns from datasets.amazingmart_listoforders;
show columns from datasets.amazingmart_salestarget;

select * from datasets.amazingmart_listoforders;
select * from datasets.amazingmart_orderbreakdown;

-- Convert all date fields from text to date / datetime
select str_to_date(Month_OrderDate, '%m-%Y') as month from datasets.amazingmart_salestarget;

select str_to_date(Ship_Date, '%d-%m-%y') from datasets.amazingmart_listoforders;

SET SQL_SAFE_UPDATES = 0;
UPDATE datasets.amazingmart_salestarget
SET Order_Date = STR_TO_DATE(Order_Date, '%d-%m-%Y');
SET SQL_SAFE_UPDATES = 1;

alter table datasets.amazingmart_listoforders
modify column Order_Date Date;

-- Update Date for Sales Target Table
select * from datasets.amazingmart_salestarget;

show columns from datasets.amazingmart_salestarget;

-- Prefix 20 for year values
SET SQL_SAFE_UPDATES = 0;
UPDATE datasets.amazingmart_salestarget
SET Month_OrderDate = CONCAT(SUBSTRING_INDEX(Month_OrderDate, '-', 1), '-20', SUBSTRING_INDEX(Month_OrderDate, '-', -1))
WHERE Month_OrderDate LIKE '%-%';  -- Ensuring it only processes entries that need this format change
SET SQL_SAFE_UPDATES = 1;

-- TRIM all spaces
SET SQL_SAFE_UPDATES = 0;
UPDATE datasets.amazingmart_salestarget
SET Month_OrderDate = TRIM(Month_OrderDate)
WHERE Month_OrderDate LIKE '%-%';
SET SQL_SAFE_UPDATES = 1;

-- Prefixing 01 for days to all date values
set SQL_SAFE_UPDATES = 0;
update datasets.amazingmart_salestarget
set Month_OrderDate = concat('01-', Month_OrderDate);
set SQL_SAFE_UPDATES = 1;

-- Convert to date format
SET SQL_SAFE_UPDATES = 0;
UPDATE datasets.amazingmart_salestarget
SET Month_OrderDate = STR_TO_DATE(Month_OrderDate, '%d-%b-%Y');
SET SQL_SAFE_UPDATES = 1;

alter table datasets.amazingmart_salestarget
modify column Month_OrderDate Date;


-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
select * from datasets.amazingmart_salestarget;

SELECT 
    Order_ID,
    Product_Name,
    Category, 
    Sales, 
    SUM(Sales) OVER (PARTITION BY Category ORDER BY Order_ID) AS RunningTotal
FROM datasets.amazingmart_orderbreakdown;


DELETE FROM datasets.amazingmart_orderbreakdown
WHERE Sales < 100;

DELETE FROM datasets.amazingmart_orderbreakdown
WHERE Category IN (
    SELECT Category
    FROM datasets.amazingmart_orderbreakdown
    GROUP BY Category
    HAVING AVG(Sales) < 200
);


-- 1. Calculate Total Sales and Profit by Category
-- Write a query to calculate the total Sales and total Profit for each Category.

select Category, sum(Sales) as Total_Sales, sum(Profit) as Total_Profit
from datasets.amazingmart_orderbreakdown
group by Category;

-- 2. Identify Orders with Discount Above Average
-- Find all Order_IDs where the Discount applied is above the average discount across all orders.

select avg(Discount) from datasets.amazingmart_orderbreakdown;

select Order_ID, Product_Name, Category, Sub_Category, Discount from datasets.amazingmart_orderbreakdown
where Discount > (select avg(Discount) from datasets.amazingmart_orderbreakdown);

with AvgDisc as (
select avg(Discount) as AvgD
from datasets.amazingmart_orderbreakdown
)
select Order_ID, Product_Name, Category, Sub_Category, Discount from datasets.amazingmart_orderbreakdown
where Discount > (select AvgD from AvgDisc);

select ob.Order_ID, ob.Product_Name, ob.Category, ob.Sub_Category, ob.Discount from datasets.mazingmart_orderbreakdown ob
join (
select avg(Discount) as AvgDisc
from datasets.amazingmart_orderbreakdown
) as avgTable
where Discount >  AvgDisc;

-- 3. Determine Profitability by Segment
-- Calculate the total Profit for each Segment across all Regions. Rank the segments based on profitability.

select a.Segment, a.Region, sum(b.Profit) as Total_Profit, rank() over(order by sum(b.Profit) desc) profitability_Rank
from datasets.amazingmart_listoforders a
join datasets.amazingmart_orderbreakdown b
on a.Order_ID = b.Order_ID
group by Segment, Region
order by Total_Profit desc;

-- 4. Sales by Region and Category
-- For each Region and Category combination, calculate the total Sales and order it by Region.

Select a.Region, b.Category, sum(b.Sales) as Total_Sales from datasets.amazingmart_listoforders a
join datasets.amazingmart_orderbreakdown b
on a.Order_ID = b.Order_ID
group by a.Region, b.Category
order by a.Region;

-- 5. Find the Percentage of Discounted Orders
-- What percentage of total orders have a non-zero Discount applied? Display the result by Segment.

select a.Segment, count(case when b.Discount > 0 then 1 end) as Discounted, count(*) as total,
count(case when b.Discount > 0 then 1 end)/count(*) * 100 as Percent
from datasets.amazingmart_listoforders a
join datasets.amazingmart_orderbreakdown b
on a.Order_ID = b.Order_ID
group by a.Segment;

with discountedorders as (
select lo.Segment, count(case when ob.Discount > 0 then 1 end) as Discounted, count(*) as TotalOrders
from datasets.amazingmart_orderbreakdown ob
join datasets.amazingmart_listoforders lo
on ob.Order_ID = lo.Order_ID
group by lo.Segment)
Select Segment, (Discounted/TotalOrders) * 100 as Percent from discountedorders;

with DiscountedOrder as (
select b.Segment, count(a.Order_ID) as DiscOrder 
from datasets.amazingmart_orderbreakdown a
join datasets.amazingmart_listoforders b
on a.Order_ID = b.Order_ID
where a.Discount > 0
group by b.Segment
),
TotalOrders as (
Select b.Segment, count(a.Order_ID) as TotOrder
from datasets.amazingmart_orderbreakdown a
join datasets.amazingmart_listoforders b
on a.Order_ID = b.Order_ID
group by b.Segment
)
select t.Segment, (d.DiscOrder/t.TotOrder)*100 as Percentage from DiscountedOrder d
join TotalOrders t on t.Segment = d.Segment
group by t.Segment;

-- 6. Average Sales Per Product and Sub-Category
-- Write a query to get the average Sales for each Product_Name within each Sub_Category.

select Sub_Category, Product_Name, avg(Sales) as AvgSales from datasets.amazingmart_orderbreakdown
group by Sub_Category, Product_Name
order by Sub_Category, Product_Name;

-- 7. Identify Late Shipments
-- List all orders where the Ship_Date is more than 7 days after the Order_Date.

select Order_ID, Order_Date, Ship_Date, (Ship_Date - Order_Date) as DaystoShip
from datasets.amazingmart_listoforders
where (Ship_Date - Order_Date) > 7
order by DaystoShip asc;

-- 8. Top 10 Products by Sales in a Specific Country
-- Find the top 10 Product_Names by Sales in a specific country (e.g., 'United States').
-- Find the top 10 Product_Names by Sales for every country (NEW)

with RankedProducts as
(select a.Country, b.Product_Name, sum(b.Sales) as TotalSales, rank() over(partition by a.Country order by sum(b.Sales) desc) as SalesRank
from datasets.amazingmart_listoforders a
join amazingmart_orderbreakdown b
on a.Order_ID = b.Order_ID
group by a.Country, b.Product_Name
)
select Country, Product_Name, TotalSales, SalesRank
from RankedProducts
where SalesRank <= 10
order by Country, SalesRank;


-- 9. Calculate Monthly Sales for Each Category
-- Write a query to calculate the total Sales for each Category by month using the Order_Date and group by Month_Order_Date.

alter table datasets.amazingmart_salestarget
add column MonthName VARCHAR(20),
add column Year int,
add column Quarter int;

alter table datasets.amazingmart_salestarget add column MonthNum int;

SET SQL_SAFE_UPDATES = 0;
update datasets.amazingmart_salestarget
set MonthName = monthname(Month_OrderDate),
Year = year(Month_OrderDate),
Quarter = quarter(Month_OrderDate);
SET SQL_SAFE_UPDATES = 1;

SET SQL_SAFE_UPDATES = 0;
update datasets.amazingmart_salestarget set MonthNum = month(Month_OrderDate);
SET SQL_SAFE_UPDATES = 1;

select a.Category, concat(b.MonthName,'-', b.Year) as MonthYear, sum(a.Sales) as TotalSales from datasets.amazingmart_orderbreakdown a
join datasets.amazingmart_listoforders c
on a.Order_ID = c.Order_ID
join datasets.amazingmart_salestarget b
on date_format(c.Order_Date, '%Y-%m') = date_format(b.Month_OrderDate, '%Y-%m')
group by a.Category, MonthYear
order by a.Category;


-- 10. Find Orders Below Sales Target
-- Identify the Order_IDs where the Sales of an order for a particular Category in a month was below the Target in the
-- SalesTarget table.------- XXXXXXXXXXXXXXXXXXXX

select ob.Order_ID, ob.Category, st.MonthName, sum(ob.Sales) as Total_Sales, st.Target
from datasets.amazingmart_orderbreakdown ob
join datasets.amazingmart_listoforders lo
on ob.Order_ID = lo.Order_ID
join datasets.amazingmart_salestarget st
on ob.Category = st.Category
and date_format(lo.Order_Date, '%Y-%m') = date_format(st.MonthName, '%Y-%m')
group by ob.Order_ID, ob.Category, st.MonthName, st.Target
having Total_Sales < st.Target;

-- 11. Sales Contribution of Each Region
-- What is the contribution of each Region to the total Sales of the company? Display results as a percentage of total sales.

Select lo.Region, sum(ob.Sales) as SalesbyRegion, (select sum(Sales) as Total_Sales from amazingmart_orderbreakdown) as Total_Sales,
(sum(ob.Sales)/(select sum(Sales) as Total_Sales from amazingmart_orderbreakdown))*100 as Percent_Sales
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
group by lo.Region;

-- 12. Track Average Sales Over Time
-- Calculate the rolling 3-month average Sales by Category over time (using Order_Date).

Select ob.Category, st.Month_OrderDate, st.MonthName, st.Year, st.MonthNum, sum(ob.Sales) as Total_Sales,
avg(sum(ob.Sales)) over(partition by ob.Category order by st.Month_OrderDate range interval 2 month preceding) as Rolling_Avg_Sales
from datasets.amazingmart_orderbreakdown ob
join datasets.amazingmart_listoforders lo
on lo.Order_ID = ob.Order_ID
join datasets.amazingmart_salestarget st
on st.Category = ob.Category
and monthname(lo.Order_Date) = st.MonthName
group by ob.Category, st.Month_OrderDate, st.MonthName, st.Year, st.MonthNum
order by ob.Category, st.Year, st.MonthNum;

SELECT ob.Category, lo.Order_Date, SUM(ob.Sales) AS Total_Sales,
AVG(SUM(ob.Sales)) OVER (PARTITION BY ob.Category ORDER BY lo.Order_Date RANGE INTERVAL 2 MONTH PRECEDING)
AS Rolling_3_Month_Avg_Sales
FROM datasets.amazingmart_orderbreakdown ob
JOIN datasets.amazingmart_listoforders lo 
ON ob.Order_ID = lo.Order_ID
GROUP BY ob.Category, lo.Order_Date
ORDER BY ob.Category, lo.Order_Date;

-- 13. Highest Sales in Each Category by City
-- For each Category, find the city that contributed the most to total Sales.

with MaxSales as (
select ob.Category, lo.City, sum(ob.Sales) as MaxSalesAmt,
rank() over(partition by ob.Category order by sum(ob.Sales) desc) as MaxSalesRank from datasets.amazingmart_orderbreakdown ob
join datasets.amazingmart_listoforders lo
on lo.Order_ID = ob.Order_ID
group by ob.Category, lo.City
order by ob.Category)
select Category, City, MaxSalesAmt
from MaxSales
where MaxSalesRank = 1
Group by Category, City
order by Category, MaxSalesAmt desc;

-- 14. Total Profit by Ship Mode
-- Calculate the total Profit for each Ship_Mode and display in descending order of Profit.

Select lo.Ship_Mode, sum(ob.Profit) as Total_Profit
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
group by lo.Ship_Mode
order by Total_Profit desc;

-- 15. Find the Orders with Maximum Discount in Each Region
-- Find the Order_ID and details of orders where the maximum Discount was applied within each Region.

with maxDiscount as (
select ob.Order_ID, lo.Region, lo.Customer_Name, ob.Product_Name, ob.Category,
max(Discount) as Max_Disc, rank() over(partition by lo.Region order by max(ob.Discount) desc) as RankMaxDisc
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
group by ob.Order_ID, lo.Region, lo.Customer_Name, ob.Product_Name, ob.category
order by lo.Region
)
select Order_ID, Region, Customer_Name, Product_Name, Category, Max_Disc, RankMaxDisc
from maxDiscount
where RankMaxDisc = 1
group by Order_ID, Region, Customer_Name, Product_Name, Category;


SELECT lo.Order_ID, lo.Order_Date, lo.Customer_Name, lo.Region, ob.Product_Name, ob.Category, ob.Sales, ob.Quantity, ob.Discount
FROM datasets.amazingmart_listoforders lo
JOIN datasets.amazingmart_orderbreakdown ob 
ON lo.Order_ID = ob.Order_ID
JOIN 
    (
        SELECT lo.Region, MAX(ob.Discount) AS Max_Discount
        FROM datasets.amazingmart_listoforders lo
        JOIN datasets.amazingmart_orderbreakdown ob
        ON lo.Order_ID = ob.Order_ID
        GROUP BY lo.Region
    ) AS max_discount_per_region
ON lo.Region = max_discount_per_region.Region
AND ob.Discount = max_discount_per_region.Max_Discount;

-- 16. Product Sales Trend in a Specific Region
-- Analyze the sales trend of a specific Product_Name in a particular Region over time. Group the results by month.

select lo.Region, ob.Product_Name, date_format(lo.Order_Date, '%M-%Y') as OrderMonth, month(lo.Order_Date) as MonthNum, sum(ob.Sales) as Total_Sales
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
-- where lo.Region = '' and ob.Product_Name = ''
group by lo.Region, ob.Product_Name, OrderMonth, MonthNum
order by lo.Region, ob.Product_Name, MonthNum;

-- 17. Count of Orders for Each Country by Segment
-- Count the number of orders (Order_ID) for each Country and Segment combination.

select lo.Country, lo.Segment, count(ob.Order_ID) as Count_Orders
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
group by lo.Country, lo.Segment
order by lo.Country, lo.Segment;

-- 18. Product with the Highest Profit Margin
-- Find the Product_Name with the highest profit margin (calculated as (Profit / Sales) * 100).

select Product_Name, (sum(Profit)/sum(Sales))*100 as ProfitMargin
from datasets.amazingmart_orderbreakdown
group by Product_Name
order by ProfitMargin desc
Limit 1;

-- 19. Find Products with Zero Profit
-- Identify all Product_Names that have Profit = 0 for any Order_ID and provide details of the corresponding order.

Select Product_Name, Order_ID, Profit
from datasets.amazingmart_orderbreakdown
where Profit = 0;


-- 20. Monthly Sales Target Achievement by Category
-- For each Category, calculate the total Sales in each month and compare it with the Target from the SalesTarget table.
-- Display whether the Target was achieved or not.

select ob.Category, date_format(lo.Order_Date, '%M-%Y') as MonthYear, sum(ob.Sales) as Total_Sales, sum(st.Target) as Total_Target
from datasets.amazingmart_listoforders lo
join datasets.amazingmart_orderbreakdown ob
on lo.Order_ID = ob.Order_ID
join datasets.amazingmart_salestarget st
on st.Category = ob.Category
and date_format(lo.Order_Date, '%M-%Y') = date_format(st.Month_OrderDate, '%M-%Y')
-- where sum(ob.Sales) >= sum(st.Target)
group by ob.Category, MonthYear
order by ob.Category, MonthYear;

-- 21. Highest and lowest sales per Category
-- for each category, calculate the highest and lowest sales for a particular year



-- 22. Self Join
-- Identify all orders where the same product was purchased in different quantities by the same customer within the same
-- month. Use a self-join on the OrderBreakdown table.

select Order_ID, ;


SELECT a.Order_ID, a.Product_Name, a.Quantity, b.Customer_Name, MONTH(b.Order_Date) as Order_Month
FROM datasets.amazingmart_orderbreakdown a
JOIN datasets.amazingmart_listoforders b ON a.Order_ID = b.Order_ID
JOIN datasets.amazingmart_orderbreakdown a2 ON a.Product_Name = a2.Product_Name 
AND a.Order_ID <> a2.Order_ID
JOIN datasets.amazingmart_listoforders b2 ON a2.Order_ID = b2.Order_ID 
AND b.Customer_Name = b2.Customer_Name 
AND MONTH(b.Order_Date) = MONTH(b2.Order_Date)
WHERE a.Quantity <> a2.Quantity;


-- 23. Cross Join
-- Generate a cross product of all possible combinations between Order_ID and Category from OrderBreakdown and Customer_Name
-- from ListofOrders to see the theoretical combinations of customers and product categories.



-- 24. Inner Join
-- Write a query to retrieve all the Order_ID, Product_Name, and corresponding Target for each order from the OrderBreakdown
-- table. Use an inner join with the SalesTarget table to compare Sales and Target for each category.



-- 25. Left Join
-- Write a query to show all orders along with their corresponding sales targets. If a category does not have a target in
-- the SalesTarget table, it should still appear in the result set with a NULL target. Use a left join.


-- 26. Right Join
-- Write a query using a right join to show all records from the SalesTarget table, along with the total sales for
-- each category and month from OrderBreakdown. Display NULL for categories without sales data.



-- 27. Outer Join
-- Write a query using a full outer join to list all records from OrderBreakdown and SalesTarget, showing the difference
-- between actual sales and the target for each category, even if one of them doesn't exist in the other table.



-- 28. Wildcards with _ and %
-- Find all products whose names start with 'S' and end with 's', where the middle part can be any number of characters.
-- Use the % and _ wildcard in the query on Product_Name.


-- 29. Indexing
-- Create an index on the OrderBreakdown table for the columns Category and Sales to speed up the performance of
-- queries filtering on those columns. Write the SQL code to create this index.


-- 30. Union vs Union All
-- Write a query that retrieves all orders from the OrderBreakdown table and adds a placeholder row for the sales targets
-- for the next month. Use both UNION and UNION ALL to demonstrate the difference between removing and keeping duplicate
-- values.


-- 31. ENUM
-- You are tasked with adding a new column called Order_Status to the ListofOrders table. The column should accept only
-- three values: 'Pending', 'Shipped', and 'Cancelled'. Write the query to add this column using the ENUM data type.


-- 32. Create View
-- Create a view called Monthly_Sales_Performance that displays the total sales, target, and whether the target was met
-- for each category and month. Use data from both the OrderBreakdown and SalesTarget tables.



-- 33. Triggers
-- Write a trigger that automatically updates the Target column in the SalesTarget table when a new record is inserted.
-- The trigger should check the Category and Month_Order_Date, and if the target already exists for that category and month,
-- it should prevent the insertion.



-- 34. Regex
-- Write a query that uses REGEX to find all customer names in ListofOrders where the first name is 3 characters long,
-- and the last name starts with a vowel.



-- 35. Normalization
-- Explain how you would normalize the OrderBreakdown and ListofOrders tables to the 3rd normal form (3NF).
-- Describe the process and write the SQL queries necessary to achieve this.




-- 36: Quarterly Repeat Purchasers Across Segments
-- Identify customers who made purchases in multiple quarters across different segments.
-- Goal: For each customer who made purchases in at least two quarters and across different segments,
-- return the customer’s name, the number of unique quarters they purchased in, and the segments.


-- 37: Category-Wise Monthly Sales Target Achievement
-- Calculate the monthly sales target achievement percentage for each product category and identify which categories met or
-- exceeded the target for each month.
-- Goal: For each month, show the Category, Target, Total Sales, and Achievement (%). Indicate whether the Target was
-- achieved.


-- 38: Best-Selling Product by Profit in Each Country
-- Identify the product with the highest total profit in each country.
-- Goal: For each country, show the Product_Name, Total Profit, and Country.


-- 39: Ship Mode Preferences by Customer Segment Over Time
-- Analyze the trend of ship mode preferences within each customer segment over time, broken down by month and year.
-- Goal: For each month and year, determine the preferred Ship_Mode in each Segment based on the number of orders, and
-- display the count of each mode.


-- 40: Impact of Discounts on Profitability by Sub-Category
-- Determine whether higher discounts are associated with reduced profitability for each sub-category.
-- Goal: For each sub-category, calculate the average Discount, Total Sales, and Total Profit, then analyze whether higher
-- discounts correlate with lower profits.


-- 41: Identifying Seasonal Trends in Product Category Sales
-- Identify categories with significant seasonal trends based on quarterly sales performance.
-- Goal: For each quarter, find the product categories that have a significant increase or decrease in sales compared to the
-- previous quarter, and calculate the percentage change.


-- 42: Regional Analysis of Target Achievement by Quarter
-- Compare quarterly sales to targets in each region, and identify regions that consistently meet or exceed targets.
-- Goal: For each region, calculate the quarterly Sales, Target, and achievement percentage. Highlight regions with at
-- least three quarters where sales met or exceeded the target.


-- 43: Profit Contribution Analysis of Top 10% Products
-- Analyze the contribution of the top 10% most profitable products to the total profit within each category.
-- Goal: For each category, determine the top 10% of products by profit and calculate their total profit contribution
-- relative to the total profit of the category.


