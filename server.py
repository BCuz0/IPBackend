from flask import Flask, jsonify, request
from flask_mysqldb import MySQL
import MySQLdb.cursors
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_DB'] = 'sakila'

mysql = MySQL(app)

@app.route('/', methods=['GET'])
def top_films():
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT film.film_id, film.title, film.description, film.release_year, film.rental_rate, COUNT(*) as rented 
        FROM rental 
        INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
        INNER JOIN film ON inventory.film_id = film.film_id
        GROUP BY film_id 
        ORDER BY rented DESC 
        LIMIT 5;
    """
    cursor.execute(query)
    top_films = cursor.fetchall()
    cursor.close()
    return jsonify(top_films)

@app.route('/top_actors', methods=['GET'])
def top_actors():
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(film_actor.film_id) as films_count 
        FROM film_actor
        INNER JOIN actor ON film_actor.actor_id = actor.actor_id
        GROUP BY actor_id
        ORDER BY films_count DESC
        LIMIT 5;
    """
    cursor.execute(query)
    top_actors = cursor.fetchall()
    return jsonify(top_actors)

@app.route('/top_actor_films/<int:actor_id>', methods=['GET'])
def actors_top_films(actor_id):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT film.film_id, film.title, COUNT(rental.rental_id) as rented
        FROM film_actor
        INNER JOIN film ON film_actor.film_id = film.film_id
        INNER JOIN inventory ON film.film_id = inventory.film_id
        INNER JOIN rental ON inventory.inventory_id = rental.inventory_id
        WHERE film_actor.actor_id = %s
        GROUP BY film.film_id, film.title
        ORDER BY rented DESC
        LIMIT 5;
    """
    cursor.execute(query, (actor_id,))
    actor_films = cursor.fetchall()
    cursor.close()
    return jsonify(actor_films)

@app.route('/search', methods=['GET'])
def search_films():
    search_query = request.args.get('query', '').strip()
    if not search_query:
        return jsonify([])
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT DISTINCT film.film_id, film.title, film.description, film.release_year, film.rental_rate
        FROM film
        LEFT JOIN film_actor ON film.film_id = film_actor.film_id
        LEFT JOIN actor ON film_actor.actor_id = actor.actor_id
        LEFT JOIN film_category ON film.film_id = film_category.film_id
        LEFT JOIN category ON film_category.category_id = category.category_id
        WHERE film.title LIKE %s 
        OR CONCAT(actor.first_name, ' ', actor.last_name) LIKE %s 
        OR category.name LIKE %s
    """
    search_term = f"%{search_query}%"
    cursor.execute(query, (search_term, search_term, search_term))
    search_results = cursor.fetchall()
    cursor.close()
    return jsonify(search_results)

@app.route('/films', methods=['GET'])
def get_films():
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 5, type=int)
    offset = (page - 1) * limit

    cursor.execute("SELECT COUNT(*) AS total FROM film")
    total_films = cursor.fetchone()['total']

    query = """
        SELECT film_id, title, description, release_year, rental_rate
        FROM film
        ORDER BY title ASC
        LIMIT %s OFFSET %s
    """
    cursor.execute(query, (limit, offset))
    films = cursor.fetchall()
    cursor.close()

    return jsonify({
        "films": films,
        "total_films": total_films,
        "page": page,
        "limit": limit
    })

@app.route('/film/<int:film_id>', methods=['GET'])
def get_film_details(film_id):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT film.film_id, film.title, film.description, film.release_year, film.rental_rate
        FROM film
        WHERE film.film_id = %s
    """
    cursor.execute(query, (film_id,))
    film = cursor.fetchone()
    cursor.close()
    return jsonify(film)

@app.route('/film_availability/<int:film_id>', methods=['GET'])
def get_film_availability(film_id):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    query = """
        SELECT COUNT(inventory.inventory_id) AS available_copies
        FROM inventory
        LEFT JOIN rental ON inventory.inventory_id = rental.inventory_id 
            AND rental.return_date IS NULL
        WHERE inventory.film_id = %s 
        AND rental.rental_id IS NULL;
    """
    cursor.execute(query, (film_id,))
    result = cursor.fetchone()
    cursor.close()
    return jsonify({"available_copies": result['available_copies']})

@app.route('/validate_customer/<int:customer_id>', methods=['GET'])
def validate_customer(customer_id):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
    customer = cursor.fetchone()
    cursor.close()
    return jsonify({"exists": bool(customer)})

@app.route('/rent_film', methods=['POST'])
def rent_film():
    data = request.json
    film_id = data.get('film_id')
    customer_id = data.get('customer_id')
    if not customer_id or not film_id:
        return jsonify({"error": "Customer ID and Film ID are required."}), 400

    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
    customer = cursor.fetchone()
    if not customer:
        return jsonify({"error": "Invalid customer ID."}), 400
    cursor.execute("""
        SELECT COUNT(*) AS available_copies 
        FROM inventory 
        WHERE film_id = %s 
        AND inventory_id NOT IN (SELECT inventory_id FROM rental WHERE return_date IS NULL)
    """, (film_id,))
    result = cursor.fetchone()
    available_copies = result['available_copies'] if result else 0
    if available_copies < 1:
        return jsonify({"error": "No available copies for this film."}), 400

    cursor.execute("""
        SELECT inventory_id FROM inventory 
        WHERE film_id = %s 
        AND inventory_id NOT IN (SELECT inventory_id FROM rental WHERE return_date IS NULL)
        LIMIT 1
    """, (film_id,))
    inventory = cursor.fetchone()
    inventory_id = inventory['inventory_id']
    cursor.execute("""
        INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id) 
        VALUES (NOW(), %s, %s, 1)
    """, (inventory_id, customer_id))
    
    mysql.connection.commit()
    cursor.close()
    return jsonify({"message": "Rental successful"})

@app.route('/customers', methods=['GET'])
def get_customers():
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 10, type=int)
    search = request.args.get('search', '').strip()

    offset = (page - 1) * limit

    query = """
        SELECT customer_id, first_name, last_name, email, create_date
        FROM customer
    """
    conditions = []
    params = []

    if search.isdigit():
        conditions.append("customer_id = %s")
        params.append(search)
    else:
        conditions.append("(first_name LIKE %s OR last_name LIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY customer_id ASC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    cursor.execute(query, tuple(params))
    customers = cursor.fetchall()
    count_query = "SELECT COUNT(*) AS total FROM customer"
    if conditions:
        count_query += " WHERE " + " AND ".join(conditions)
    cursor.execute(count_query, tuple(params[:-2]))  # Exclude limit and offset params for count
    total_customers = cursor.fetchone()["total"]

    cursor.close()

    return jsonify({
        "customers": customers,
        "total_customers": total_customers,
        "page": page,
        "limit": limit
    })
@app.route('/customers', methods=['POST'])
def add_customer():
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    data = request.json
    if not all(key in data for key in ['first_name', 'last_name', 'email']):
        return jsonify({"success": False, "message": "Missing required fields"}), 400

    query = "INSERT INTO customer (first_name, last_name, email, address_id, store_id, active, create_date) VALUES (%s, %s, %s, %s, 1, 1, NOW())"
    values = (data['first_name'], data['last_name'], data['email'], data['address_id'])

    try:
        cursor.execute(query, values)
        mysql.connection.commit()
        return jsonify({"success": True, "message": "Customer added successfully"})
    except MySQLdb.Error as err:
        print(f"Error: {err}")
        return jsonify({"success": False, "message": str(err)}), 500
    finally:
        cursor.close()
@app.route('/customers/<int:customer_id>', methods=['PUT'])
def update_customer(customer_id):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    data = request.json

    if not all(key in data for key in ['first_name', 'last_name', 'email', 'address_id']):
        return jsonify({"success": False, "message": "Missing required fields"}), 400

    query = """
        UPDATE customer 
        SET first_name = %s, last_name = %s, email = %s, address_id = %s 
        WHERE customer_id = %s
    """
    values = (data['first_name'], data['last_name'], data['email'], data['address_id'], customer_id)

    try:
        cursor.execute(query, values)
        mysql.connection.commit()
        return jsonify({"success": True, "message": "Customer updated successfully"})
    except MySQLdb.Error as err:
        return jsonify({"success": False, "message": str(err)}), 500
    finally:
        cursor.close()
@app.route('/customers/<int:customer_id>', methods=['DELETE'])
def delete_customer(customer_id):
    try:
        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("SELECT customer_id FROM customer WHERE customer_id = %s", (customer_id,))
        customer = cursor.fetchone()
        if not customer:
            return jsonify({"success": False, "message": "Customer not found"}), 404

        cursor.execute("DELETE FROM customer WHERE customer_id = %s", (customer_id,))
        mysql.connection.commit()
        cursor.close()
        return jsonify({"success": True, "message": "Customer deleted successfully"})

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
@app.route('/customers/<int:customer_id>/rental-history', methods=['GET'])
def get_customer_rental_history(customer_id):
    try:
        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute("""
            SELECT f.title, r.rental_date, r.return_date
            FROM rental r
            JOIN inventory i ON r.inventory_id = i.inventory_id
            JOIN film f ON i.film_id = f.film_id
            WHERE r.customer_id = %s
            ORDER BY r.rental_date DESC
        """, (customer_id,))
        
        rentals = cursor.fetchall()
        cursor.close()
        return jsonify({"rentals": rentals}) 
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500
if __name__ == '__main__':
    app.run(debug=True)