const fs = require('fs')
const {join} = require('path')
const mysql = require('mysql2')

const pull = require('pull-stream')
const toPull = require('stream-to-pull-stream')

const Queries = require('./queries')

const connection = mysql.createConnection(Object.assign(require('../credentials.json'), {
  dateStrings: true,
  ssl: {
    ca: fileContent('ca-cert.pem'),
    cert: fileContent('client-cert.pem'),
    key: fileContent('client-key.pem')
  }
}))

const {bookings} = Queries(connection)

const from = '2023-10-02 00:00:00'
const to   = '2023-10-03 00:00:00'

const stream = bookings(from, to)
  .stream({highWaterMark: 50})

pull(
  toPull.source(stream),

  pull.drain(({id, created_at, product_name}) => {
    console.log(`${id} ${created_at} ${product_name}`)
  })
)

connection.end()

// -- util


function fileContent(fname) {
  return fs.readFileSync(join(__dirname, '..', fname))
}
