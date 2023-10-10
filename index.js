const fs = require('fs')
const {join} = require('path')
const mysql = require('mysql2')

const pull = require('pull-stream')
const toPull = require('stream-to-pull-stream')

const Queries = require('./queries')

const connectionOpts = Object.assign(require('../credentials.json'), {
  dateStrings: true,
  connectionLimit: 10,
  ssl: {
    ca: fileContent('ca-cert.pem'),
    cert: fileContent('client-cert.pem'),
    key: fileContent('client-key.pem')
  }
})

const connection = mysql.createConnection(connectionOpts)

const {bookings, entries} = Queries(connection)

const from = '2023-10-02 00:00:00'
const to   = '2023-10-03 00:00:00'

function showBookings() {
  pull(
    src(bookings(from, to)),
    pull.drain(({id, created_at, product_name}) => {
      console.log(`${id} ${created_at} ${product_name}`)
    })
  )
}

function showEntries() {
  pull(
    src(entries(from, to)),
    pull.drain(({foreign_type, foreign_id, entry_at, people_count, position, booking_id, start_time}) => {
      console.log(`${entry_at} ${people_count}x ${foreign_type}#${foreign_id} @${Buffer.from(position, 'base64').toString()} ${booking_id} ${start_time}`)
    })
  )
}

showEntries()

connection.end()

// -- util

function src(s) {
  const stream = s.stream({highWaterMark: 50})
  return toPull.source(stream)
}

function fileContent(fname) {
  return fs.readFileSync(join(__dirname, '..', fname))
}
