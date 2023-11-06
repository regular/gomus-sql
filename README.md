Needs patch in mysql2/lib/connection.js
or a proper cert.

```
    const secureSocket = Tls.connect({
      rejectUnauthorized,
      requestCert: rejectUnauthorized,
      secureContext,
      isServer: false,
      socket: this.stream,
      servername,
      // patched here, will go away after npm install
      checkServerIdentity: (host, cert) =>{
        //console.log('XXX', host, cert)
      }
```
