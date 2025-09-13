const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const config = require('./config');

const app = express();
const PORT = config.port;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const db = mysql.createConnection(config.database);

db.connect((err) => {
  if (err) {
    console.error('Database connection failed:', err);
  } else {
    console.log('Connected to Sakila database');
  }
});

// Feature 1: Get top 5 rented films of all time
app.get('/api/top-rented-films', (req, res) => {
  const query = `
    SELECT 
      f.film_id,
      f.title,
      f.description,
      f.release_year,
      f.rating,
      f.rental_rate,
      COUNT(r.rental_id) as rental_count
    FROM film f
    JOIN inventory inv ON inv.film_id = f.film_id
    JOIN rental r ON r.inventory_id = inv.inventory_id
    GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate
    ORDER BY rental_count DESC
    LIMIT 5
  `;
  
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching top rented films:', err);
      res.status(500).json({ error: 'Failed to fetch top rented films' });
    } else {
      console.log('Top rented films query successful, returned', results.length, 'results');
      res.json(results);
    }
  });
});

// Feature 2: Get detailed film information
app.get('/api/film/:id', (req, res) => {
  const filmId = req.params.id;
  
  const query = `
    SELECT 
      f.film_id,
      f.title,
      f.description,
      f.release_year,
      f.rating,
      f.rental_rate,
      f.rental_duration,
      f.length,
      f.replacement_cost,
      f.special_features,
      l.name as language,
      GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') as categories,
      GROUP_CONCAT(DISTINCT CONCAT(a.first_name, ' ', a.last_name) ORDER BY a.last_name SEPARATOR ', ') as actors,
      COUNT(DISTINCT r.rental_id) as rental_count,
      COUNT(DISTINCT i.inventory_id) as total_copies,
      COUNT(DISTINCT CASE WHEN r.return_date IS NULL THEN r.rental_id END) as currently_rented
    FROM film f
    LEFT JOIN language l ON f.language_id = l.language_id
    LEFT JOIN film_category fc ON f.film_id = fc.film_id
    LEFT JOIN category c ON fc.category_id = c.category_id
    LEFT JOIN film_actor fa ON f.film_id = fa.film_id
    LEFT JOIN actor a ON fa.actor_id = a.actor_id
    LEFT JOIN inventory i ON f.film_id = i.film_id
    LEFT JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE f.film_id = ?
    GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate, f.rental_duration, f.length, f.replacement_cost, f.special_features, l.name
  `;
  
  db.query(query, [filmId], (err, results) => {
    if (err) {
      console.error('Error fetching film details:', err);
      res.status(500).json({ error: 'Failed to fetch film details' });
    } else if (results.length === 0) {
      res.status(404).json({ error: 'Film not found' });
    } else {
      res.json(results[0]);
    }
  });
});

// Feature 3: Get top 5 actors from store films
app.get('/api/top-actors', (req, res) => {
  const query = `
    SELECT 
      a.actor_id, 
      a.first_name, 
      a.last_name, 
      COUNT(filmactor.film_id) AS film_count
    FROM actor a
    JOIN film_actor filmactor ON a.actor_id = filmactor.actor_id
    GROUP BY a.actor_id, a.first_name, a.last_name
    ORDER BY film_count DESC
    LIMIT 5
  `;
  
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching top actors:', err);
      res.status(500).json({ error: 'Failed to fetch top actors' });
    } else {
      console.log('Top actors query successful, returned', results.length, 'results');
      res.json(results);
    }
  });
});

// Feature 4: Get detailed actor information and their top 5 rented films
app.get('/api/actor/:id', (req, res) => {
  const actorId = req.params.id;
  
  const actorQuery = `
    SELECT 
      a.actor_id,
      a.first_name,
      a.last_name,
      COUNT(DISTINCT f.film_id) as total_films,
      COUNT(DISTINCT r.rental_id) as total_rentals
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    LEFT JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE a.actor_id = ?
    GROUP BY a.actor_id, a.first_name, a.last_name
  `;
  
  const topFilmsQuery = `
    SELECT 
      f.film_id,
      f.title,
      f.description,
      f.release_year,
      f.rating,
      f.rental_rate,
      COUNT(r.rental_id) as rental_count
    FROM actor a
    JOIN film_actor fa ON a.actor_id = fa.actor_id
    JOIN film f ON fa.film_id = f.film_id
    JOIN inventory i ON f.film_id = i.film_id
    JOIN rental r ON i.inventory_id = r.inventory_id
    WHERE a.actor_id = ?
    GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate
    ORDER BY rental_count DESC
    LIMIT 5
  `;
  
  // Get actor details
  db.query(actorQuery, [actorId], (err, actorResults) => {
    if (err) {
      console.error('Error fetching actor details:', err);
      res.status(500).json({ error: 'Failed to fetch actor details' });
    } else if (actorResults.length === 0) {
      res.status(404).json({ error: 'Actor not found' });
    } else {
      const actor = actorResults[0];
      
      // Get actor's top 5 rented films
      db.query(topFilmsQuery, [actorId], (err, filmsResults) => {
        if (err) {
          console.error('Error fetching actor films:', err);
          res.status(500).json({ error: 'Failed to fetch actor films' });
        } else {
          res.json({
            actor: actor,
            topFilms: filmsResults
          });
        }
      });
    }
  });
});

// Feature 5: Search films by name, actor, or genre
app.get('/api/search-films', (req, res) => {
  const { query, type } = req.query;
  
  if (!query || !type) {
    return res.status(400).json({ error: 'Query and type parameters are required' });
  }
  
  let searchQuery = '';
  let searchParams = [];
  
  switch (type) {
    case 'title':
      searchQuery = `
        SELECT DISTINCT
          f.film_id,
          f.title,
          f.description,
          f.release_year,
          f.rating,
          f.rental_rate,
          COUNT(r.rental_id) as rental_count
        FROM film f
        LEFT JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE f.title LIKE ?
        GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate
        ORDER BY f.title
      `;
      searchParams = [`%${query}%`];
      break;
      
    case 'actor':
      searchQuery = `
        SELECT DISTINCT
          f.film_id,
          f.title,
          f.description,
          f.release_year,
          f.rating,
          f.rental_rate,
          COUNT(r.rental_id) as rental_count
        FROM film f
        JOIN film_actor fa ON f.film_id = fa.film_id
        JOIN actor a ON fa.actor_id = a.actor_id
        LEFT JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE CONCAT(a.first_name, ' ', a.last_name) LIKE ?
        GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate
        ORDER BY f.title
      `;
      searchParams = [`%${query}%`];
      break;
      
    case 'genre':
      searchQuery = `
        SELECT DISTINCT
          f.film_id,
          f.title,
          f.description,
          f.release_year,
          f.rating,
          f.rental_rate,
          COUNT(r.rental_id) as rental_count
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        JOIN category c ON fc.category_id = c.category_id
        LEFT JOIN inventory i ON f.film_id = i.film_id
        LEFT JOIN rental r ON i.inventory_id = r.inventory_id
        WHERE c.name LIKE ?
        GROUP BY f.film_id, f.title, f.description, f.release_year, f.rating, f.rental_rate
        ORDER BY f.title
      `;
      searchParams = [`%${query}%`];
      break;
      
    default:
      return res.status(400).json({ error: 'Invalid search type. Use: title, actor, or genre' });
  }
  
  db.query(searchQuery, searchParams, (err, results) => {
    if (err) {
      console.error('Error searching films:', err);
      res.status(500).json({ error: 'Failed to search films' });
    } else {
      console.log(`Film search (${type}: "${query}") successful, returned`, results.length, 'results');
      res.json(results);
    }
  });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
