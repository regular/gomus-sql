//jshint esversion: 11
//jshint -W033
const fs = require('fs')
const {join} = require('path')
const mysql = require('mysql2')

const pull = require('pull-stream')
const toPull = require('stream-to-pull-stream')
const defer = require('pull-defer')
const merge = require('pull-merge')

const Queries = require('./queries')
const Enums = require('./enums')

const connectionOpts = Object.assign(require('../credentials.json'), {
  dateStrings: true,
  ssl: {
    ca: fileContent('ca-cert.pem'),
    cert: fileContent('client-cert.pem'),
    key: fileContent('client-key.pem')
  }
})

const pool = mysql.createPool(connectionOpts)

const {bookings, entries} = Queries(execute)
const {update} = Enums(execute)

//const from = '2023-10-02 00:00:00'
const from = '2023-10-02 00:00:00'
const to   = '2023-10-03 10:30:00'

update( (err, enums)=>{
  console.log(err, enums)
  showEntries(from, to, err=>{
    console.log('Pool end')
    pool.end()
  })
})

/*
showEntries(from, to, err=>{
  console.log('Pool end')
  pool.end()
})
*/

// -- util

function showBookings() {
  pull(
    src(bookings(from, to)),
    pull.drain(({id, created_at, product_name}) => {
      console.log(`${id} ${created_at} ${product_name}`)
    })
  )
}

function showEntries(from, to, cb) {
  pull(
    merge(['Booking', 'BookingSeat', 'TicketSale'].map(bc_type=>entries(from, to, bc_type)), compareEntries),
    /*pull.drain(({foreign_type, foreign_id, entry_at, people_count, position, booking_id, start_time, product_name, address_country_id}) => {
      console.log(`${entry_at} ${people_count}x ${foreign_type}#${foreign_id} @${position ? Buffer.from(position, 'base64').toString():'n/a'} ${booking_id} ${start_time} ${product_name} ${address_country_id}`)
    }, cb)*/
    pull.log(cb)
  )
}

function execute(sql, params) {
  const ret = defer.source()
  pool.getConnection( (err, conn) => {
    if (err) return ret.resolve(pull.error(err))

    const stream = conn.execute(sql, params).stream({highWaterMark: 50})
    stream.on('end', ()=>{
      console.log('release conn')
      pool.releaseConnection(conn)
    })

    ret.resolve( toPull.source(stream) )
  })
  return ret
}

function compareEntries(a, b) {
  a = a.entry_at
  b = b.entry_at
  if (a>b) return 1
  if (a<b) return -1
  return 0
}

function fileContent(fname) {
  return fs.readFileSync(join(__dirname, '..', fname))
}
