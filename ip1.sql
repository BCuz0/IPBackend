USE sakila;
-- Query 1
SELECT FID as film_id, title, category FROM film_list;
-- Query 2
 SELECT category, count(*) FROM film_list GROUP BY category;
-- Query 3
SELECT film_actor.actor_id, actor.first_name, actor.last_name, COUNT(*) as movies FROM film_actor
INNER JOIN
actor ON film_actor.actor_id=actor.actor_id GROUP BY actor_id ORDER BY COUNT(*) DESC;
-- Query 4
SELECT store_id, film_id, count(inventory_id) as DVD FROM inventory
GROUP BY store_id, film_id;
-- Query 5
SELECT rental_id, rental_date, inventory_id, customer_id, return_date, staff_id, last_update FROM rental LIMIT 1000;
-- Query 6
SELECT film_list.title, COUNT(*) as rented FROM rental
INNER JOIN inventory on inventory.inventory_id=rental.inventory_id
INNER JOIN film_list  on inventory.film_id=film_list.FID
GROUP BY film_id
ORDER BY rented DESC LIMIT 5;
-- Query 7
WITH highest_film_actor AS (
SELECT actor_id FROM film_actor
GROUP BY actor_id ORDER BY COUNT(film_id) DESC
LIMIT 1
),
actors_films AS (
SELECT film_actor.film_id, film.title FROM film_actor
INNER JOIN film ON film_actor.film_id = film.film_id
WHERE film_actor.actor_id = (SELECT actor_id FROM highest_film_actor)
),
rental_count AS (
SELECT actors_films.film_id, actors_films.title, COUNT(rental.rental_id) AS rented FROM actors_films
INNER JOIN inventory on actors_films.film_id = inventory.film_id
INNER JOIN rental on inventory.inventory_id = rental.inventory_id
GROUP BY actors_films.film_id, actors_films.title
)
SELECT film_id, title, rented FROM rental_count
ORDER BY rented DESC LIMIT 5;
-- Query 8
SELECT rental.customer_id, customer.first_name, customer.last_name, COUNT(*) as count FROM rental
INNER JOIN customer ON customer.customer_id=rental.customer_id
GROUP BY customer_id ORDER BY count DESC;