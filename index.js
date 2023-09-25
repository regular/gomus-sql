const fs = require('fs')
const {join} = require('path')
const mysql = require('mysql')

const pull = require('pull-stream')
const toPull = require('stream-to-pull-stream')

//const sql = 'select id, name_intern from product_categories_view'

const sql = `
SELECT
  b.id,
  product_id,
  p.name AS product_name,
  l.name AS language_name,
  c.name AS country_name,
  ca.zip AS zipcode,
  p.product_category_id,
  pc.name AS product_category_name,
  b.created_at,
  b.canceled_at,
  b.updated_at,
  b.confirmed_at,
  customer_adress_id,
  b.customer_id,
  language_id,
  start_time
FROM bookings_view b
JOIN products_view p ON
b.product_id = p.id
JOIN product_categories_view pc ON
pc.id = p.product_category_id
JOIN languages_view l ON
b.language_id = l.id 
JOIN customer_adresses_view ca ON
b.customer_adress_id = ca.id
JOIN countries_view c ON
ca.country_id = c.id
WHERE b.created_at < '2022-01-01 00:00:00'
ORDER BY b.created_at
`

//const sql = 'select * from products_view'

const connection = mysql.createConnection(Object.assign(require('../credentials.json'), {
  ssl: {
    ca: fileContent('ca-cert.pem'),
    cert: fileContent('client-cert.pem'),
    key: fileContent('client-key.pem')
  }
}))

const stream = connection.query({sql, typeCast: (field, next)=>`[${field.type}] ${field.string()}`})
  .stream({highWaterMark: 5})

pull(
  toPull.source(stream),
  pull.log()
)

connection.end()

// -- util

function fileContent(fname) {
  return fs.readFileSync(join(__dirname, '..', fname))
}
