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

// Feature 7: Get customers for rental (simple list for rental modal)
app.get('/api/customers-simple', (req, res) => {
  const query = `
    SELECT 
      customer_id,
      first_name,
      last_name,
      email,
      active
    FROM customer
    WHERE active = 1
    ORDER BY last_name, first_name
  `;
  
  db.query(query, (err, results) => {
    if (err) {
      console.error('Error fetching customers:', err);
      res.status(500).json({ error: 'Failed to fetch customers' });
    } else {
      console.log('Customers query successful, returned', results.length, 'results');
      res.json(results);
    }
  });
});

// Feature 7: Get available inventory for a film
app.get('/api/film/:id/inventory', (req, res) => {
  const filmId = req.params.id;
  
  const query = `
    SELECT 
      i.inventory_id,
      i.store_id,
      s.store_id,
      s.manager_staff_id,
      CASE 
        WHEN r.rental_id IS NOT NULL AND r.return_date IS NULL THEN 'Rented'
        ELSE 'Available'
      END as status,
      r.return_date
    FROM inventory i
    JOIN store s ON i.store_id = s.store_id
    LEFT JOIN rental r ON i.inventory_id = r.inventory_id AND r.return_date IS NULL
    WHERE i.film_id = ?
    ORDER BY i.store_id, i.inventory_id
  `;
  
  db.query(query, [filmId], (err, results) => {
    if (err) {
      console.error('Error fetching film inventory:', err);
      res.status(500).json({ error: 'Failed to fetch film inventory' });
    } else {
      console.log(`Film inventory query successful for film ${filmId}, returned`, results.length, 'results');
      res.json(results);
    }
  });
});

// Feature 7: Create a rental
app.post('/api/rentals', (req, res) => {
  const { customer_id, inventory_id, staff_id } = req.body;
  
  if (!customer_id || !inventory_id || !staff_id) {
    return res.status(400).json({ error: 'customer_id, inventory_id, and staff_id are required' });
  }
  
  // Check if inventory is available
  const checkQuery = `
    SELECT i.inventory_id 
    FROM inventory i
    LEFT JOIN rental r ON i.inventory_id = r.inventory_id AND r.return_date IS NULL
    WHERE i.inventory_id = ? AND r.rental_id IS NULL
  `;
  
  db.query(checkQuery, [inventory_id], (err, results) => {
    if (err) {
      console.error('Error checking inventory availability:', err);
      return res.status(500).json({ error: 'Failed to check inventory availability' });
    }
    
    if (results.length === 0) {
      return res.status(400).json({ error: 'Inventory item is not available for rental' });
    }
    
    // Create the rental
    const rentalQuery = `
      INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id, return_date)
      VALUES (NOW(), ?, ?, ?, NULL)
    `;
    
    db.query(rentalQuery, [inventory_id, customer_id, staff_id], (err, result) => {
      if (err) {
        console.error('Error creating rental:', err);
        res.status(500).json({ error: 'Failed to create rental' });
      } else {
        console.log(`Rental created successfully for customer ${customer_id}, inventory ${inventory_id}`);
        res.json({ 
          success: true, 
          rental_id: result.insertId,
          message: 'Film rented successfully'
        });
      }
    });
  });
});

// Get customers with pagination and search
app.get('/api/customers', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limit = parseInt(req.query.limit) || 10;
  const offset = (page - 1) * limit;
  const search = req.query.search || '';
  const type = req.query.type || 'name';
  
  // Build search conditions
  let searchCondition = 'WHERE active = 1';
  let searchParams = [];
  
  if (search.trim()) {
    switch (type) {
      case 'id':
        searchCondition += ' AND customer_id = ?';
        searchParams.push(parseInt(search.trim()));
        break;
      case 'first_name':
        searchCondition += ' AND first_name LIKE ?';
        searchParams.push(`%${search.trim()}%`);
        break;
      case 'last_name':
        searchCondition += ' AND last_name LIKE ?';
        searchParams.push(`%${search.trim()}%`);
        break;
      case 'name':
      default:
        searchCondition += ' AND (first_name LIKE ? OR last_name LIKE ?)';
        searchParams.push(`%${search.trim()}%`, `%${search.trim()}%`);
        break;
    }
  }
  
  // Get total count with search
  const countQuery = `SELECT COUNT(*) as total FROM customer ${searchCondition}`;
  
  db.query(countQuery, searchParams, (err, countResult) => {
    if (err) {
      console.error('Error getting customer count:', err);
      return res.status(500).json({ error: 'Failed to get customer count' });
    }
    
    const total = countResult[0].total;
    const totalPages = Math.ceil(total / limit);
    
    // Get customers with pagination and search
    const customersQuery = `
      SELECT 
        customer_id,
        store_id,
        first_name,
        last_name,
        email,
        address_id,
        active,
        create_date,
        last_update
      FROM customer 
      ${searchCondition}
      ORDER BY last_name, first_name
      LIMIT ? OFFSET ?
    `;
    
    const queryParams = [...searchParams, limit, offset];
    
    db.query(customersQuery, queryParams, (err, result) => {
      if (err) {
        console.error('Error getting customers:', err);
        return res.status(500).json({ error: 'Failed to get customers' });
      }
      
      console.log(`Customer search (${type}: "${search}") successful, returned`, result.length, 'results');
      
      res.json({
        customers: result,
        pagination: {
          currentPage: page,
          totalPages: totalPages,
          totalCustomers: total,
          limit: limit,
          hasNext: page < totalPages,
          hasPrev: page > 1
        }
      });
    });
  });
});

// Add new customer
app.post('/api/customers', (req, res) => {
  const { first_name, last_name, email, store_id } = req.body;
  
  if (!first_name || !last_name || !email) {
    return res.status(400).json({ error: 'First name, last name, and email are required' });
  }
  
  // Check if email already exists
  const checkEmailQuery = 'SELECT customer_id FROM customer WHERE email = ?';
  
  db.query(checkEmailQuery, [email], (err, results) => {
    if (err) {
      console.error('Error checking email:', err);
      return res.status(500).json({ error: 'Failed to check email' });
    }
    
    if (results.length > 0) {
      return res.status(400).json({ error: 'Email already exists' });
    }
    
    // Create new customer
    const insertQuery = `
      INSERT INTO customer (store_id, first_name, last_name, email, address_id, active, create_date, last_update)
      VALUES (?, ?, ?, ?, 1, 1, NOW(), NOW())
    `;
    
    const values = [store_id || 1, first_name, last_name, email];
    
    db.query(insertQuery, values, (err, result) => {
      if (err) {
        console.error('Error creating customer:', err);
        return res.status(500).json({ error: 'Failed to create customer' });
      }
      
      console.log(`Customer created successfully: ${first_name} ${last_name} (ID: ${result.insertId})`);
      res.json({ 
        success: true, 
        customer_id: result.insertId,
        message: 'Customer created successfully'
      });
    });
  });
});

// Update customer
app.put('/api/customers/:id', (req, res) => {
  const customerId = req.params.id;
  const { first_name, last_name, email, store_id } = req.body;
  
  if (!first_name || !last_name || !email) {
    return res.status(400).json({ error: 'First name, last name, and email are required' });
  }
  
  // Check if customer exists
  const checkCustomerQuery = 'SELECT customer_id FROM customer WHERE customer_id = ?';
  
  db.query(checkCustomerQuery, [customerId], (err, results) => {
    if (err) {
      console.error('Error checking customer:', err);
      return res.status(500).json({ error: 'Failed to check customer' });
    }
    
    if (results.length === 0) {
      return res.status(404).json({ error: 'Customer not found' });
    }
    
    // Check if email already exists for another customer
    const checkEmailQuery = 'SELECT customer_id FROM customer WHERE email = ? AND customer_id != ?';
    
    db.query(checkEmailQuery, [email, customerId], (err, emailResults) => {
      if (err) {
        console.error('Error checking email:', err);
        return res.status(500).json({ error: 'Failed to check email' });
      }
      
      if (emailResults.length > 0) {
        return res.status(400).json({ error: 'Email already exists for another customer' });
      }
      
      // Update customer
      const updateQuery = `
        UPDATE customer 
        SET first_name = ?, last_name = ?, email = ?, store_id = ?, last_update = NOW()
        WHERE customer_id = ?
      `;
      
      const values = [first_name, last_name, email, store_id || 1, customerId];
      
      db.query(updateQuery, values, (err, result) => {
        if (err) {
          console.error('Error updating customer:', err);
          return res.status(500).json({ error: 'Failed to update customer' });
        }
        
        console.log(`Customer updated successfully: ${first_name} ${last_name} (ID: ${customerId})`);
        res.json({ 
          success: true, 
          message: 'Customer updated successfully'
        });
      });
    });
  });
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
