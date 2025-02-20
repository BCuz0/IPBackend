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

@app.route('/rent', methods=['POST'])
def rent_film():
    try:
        data = request.get_json()
        film_id = data['film_id']
        customer_id = data['customer_id']
        staff_id = 1
        cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        query = """
            SELECT inventory_id FROM inventory
            WHERE film_id = %s
            LIMIT 1
        """
        cursor.execute(query, (film_id,))
        inventory_item = cursor.fetchone()
        if not inventory_item:
            return jsonify({'error':'No available copies'}), 400
        
        inventory_id = inventory_item['inventory_id']
        cursor.close()
        return jsonify({'message': 'Film rented'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)