import Events from 'events'
import http from 'http'
import { WebSocketServer, WebSocket } from 'ws'
import DHT from 'bittorrent-dht'
import crypto from 'crypto'

/**
 * BitTorrent tracker server.
 *
 * HTTP service which responds to GET requests from torrent clients. Requests include
 * metrics from clients that help the tracker keep overall statistics about the torrent.
 * Responses include a peer list that helps the client participate in the torrent.
 *
 * @param {Object}  opts                options object
 * @param {Object}  opts.timer       interval for general things like checking for active and inactive connections (ms)
 * @param {String}  opts.host     host used for server
 * @param {Number}  opts.port     port used for server
 * @param {String}  opts.domain     domain name that will be used
 * @param {String}  opts.hashes     join the relays for these hashes, array of hashes or comma separated string of hashes
 * @param {Object} opts.limit       limit the connections of the relay and the hashes
 * @param {Boolean}  opts.init    automatically start once instantiated
 * @param {String}  opts.server    ip of the server
 * @param {Boolean}  opts.ws    options for WebSocket Server
 */

// * @param {Function}  opts.extendRelay    have custom capabilities
// * @param {Function}  opts.extendHandler     handle custom routes
// * @param {Number}  opts.relayTimer       interval to find and connect to other trackers (ms)

export default class Server extends Events {
    constructor(opts = {}){
        super()

        const self = this
        
        // this.test = '0'
        // this.offer = null
        this.dev = Boolean(opts.dev)
        this.init = opts.init === false ? opts.init : true
        this.limit = typeof(opts.limit) === 'object' && !Array.isArray(opts.limit) ? opts.limit : {}
        this.limit.serverConnections = this.limit.serverConnections || 0
        this.limit.clientConnections = this.limit.clientConnections || 0
        this.limit.signalConnections = this.limit.signalConnections || 0
        this.timer = typeof(opts.timer) === 'object' && !Array.isArray(opts.timer) ? opts.timer : {}
        this.timer.checkServer = this.timer.checkServer || 60000
        this.timer.checkClient = this.timer.checkClient || 60000
        this.timer.talking = this.timer.talking || 1800000
        this.timer.redo = this.timer.redo || 300000
        this.http = null
        this.ws = null
        this.domain = opts.domain
        this.server = opts.server || '0.0.0.0'
        if(!opts.host){
          throw new Error('must have host')
        }
        this.host = opts.host
        this.port = opts.port || 10509
        this.address = `${this.host}:${this.port}`
        this.web = `${this.domain || this.host}:${this.port}`
        this.id = crypto.createHash('sha1').update(this.address).digest('hex')
        this.servers = new Map()
        this.clients = new Map()
        this.triedAlready = new Map()
        this.hashes = new Set(opts.hashes.split(',').filter(Boolean))
        this.relays = new Map((() => {const test = [];this.hashes.forEach((data) => {test.push([crypto.createHash('sha1').update(data).digest('hex'), []])});return test;})())
        this.offers = new Map((() => {const test = [];this.hashes.forEach((data) => {test.push([data, new Set()])});return test;})())
        // this.offers = (() => {const test = {};this.hashes.forEach((data) => {test[data] = new Map()});return test;})()

        this.http = http.createServer()
        this.http.onError = (err) => {
          this.emit('error', err)
        }
        this.http.onListening = () => {
          this.servers.forEach((soc) => {soc.send(JSON.stringify({action: 'on'}))})
          this.emit('start', 'http')
        }
        this.http.onRequest = (req, res) => {
          if(this.dev){
            console.log('http req', req.url)
          }
          if(req.method === 'HEAD' && req.url === '/'){
            res.statusCode = 200
            res.end()
          } else if(req.method === 'GET' && req.url === '/'){
            res.statusCode = 200
            res.setHeader('Content-Type', 'text/plain')
            res.end('thanks for testing bittorrent-relay')
          } else {
            res.statusCode = 400
            res.setHeader('Content-Type', 'application/json')
            res.end(JSON.stringify('invalid method or path'))
          }
        }
        this.http.onClose = () => {
          this.clients.forEach((data) => {
            data.send(JSON.stringify({action: 'relay', relay: this.randomRelay(data.hash)}))
            data.close()
          })
          this.servers.forEach((data) => {data.send(JSON.stringify({action: 'off'}))})
          this.triedAlready.clear()
          this.emit('stop', 'http')
          setTimeout(() => {this.http.listen(this.port, this.host)}, this.timer.redo)
        }
        // this.http.handleListeners = () => {
        //   this.http.off('error', this.http.onError)
        //   this.http.off('listening', this.http.onListening)
        //   this.http.off('request', this.http.onRequest)
        //   this.http.off('close', this.http.onClose)
        // }
    
        // Add default http request handler on next tick to give user the chance to add
        // their own handler first. Handle requests untouched by user's handler.
        this.ws = new WebSocketServer({
          ...(typeof(opts.ws) === 'object' && !Array.isArray(opts.ws) ? opts.ws : {}),
          perMessageDeflate: false,
          clientTracking: false,
          server: this.http
        })
        this.ws.onError = (err) => {
          this.emit('error', err)
        }
        this.ws.onConnection = (socket, req) => {
          // Note: socket.upgradeReq was removed in ws@3.0.0, so re-add it.
          // https://github.com/websockets/ws/pull/1099
    
          // if resource usage is high, send only the url of another tracker
          // else handle websockets as usual
          if(this.dev){
            console.log('ws connection', req.url)
          }
          if(req.url.startsWith('/signal?')){
            const test = new URLSearchParams(req.url.slice(req.url.indexOf('?')))
            const hasHash = test.has('hash')
            const hasId = test.has('id')
            if(!hasHash || !hasId){
              socket.send(JSON.stringify({action: 'error', error: 'must have hash and id url params'}))
              socket.close()
            } else {
              const hash = test.get('hash')
              const id = test.get('id')
              if(!this.hashes.has(hash) || this.clients.has(id)){
                socket.send(JSON.stringify({action: 'error', error: 'must have hash and url params'}))
                socket.close()
              } else {
                socket.hash = hash
                socket.id = id
                socket.active = true
                socket.ids = new Set()
                socket.web = new Set()
                this.clients.set(socket.id, socket)
                this.onClientConnection(socket)
              }
            }
          } else if(req.url.startsWith('/relay?')){
            const test = new URLSearchParams(req.url.slice(req.url.indexOf('?')))
            const hasHash = test.has('hash')
            const hasId = test.has('id')
            if(!hasHash || !hasId){
              socket.send(JSON.stringify({action: 'error', error: 'must have hash and id url params'}))
              socket.close()
            } else {
              const relay = test.get('hash')
              const id = test.get('id')
              if(!this.relays.has(relay) || this.servers.has(id)){
                socket.send(JSON.stringify({action: 'error', error: 'must have hash and id url params'}))
                socket.close()
              } else {
                if(this.limit.serverConnections){
                  if(this.relays.get(relay).length < this.limit.serverConnections){
                    socket.id = id
                    socket.server = true
                    socket.active = true
                    socket.relay = relay
                    socket.relays = []
                    this.servers.set(socket.id, socket)
                    socket.send(JSON.stringify({id: this.id, address: this.address, web: this.web, host: this.host, port: this.port, domain: this.domain, relay, action: 'session'}))
                    this.onServerConnection(socket)
                  } else {
                    socket.send(JSON.stringify({action: 'error', error: 'have reached the limit'}))
                    socket.close()
                  }
                } else {
                  socket.id = id
                  socket.server = true
                  socket.relay = relay
                  socket.relays = []
                  socket.active = true
                  this.servers.set(socket.id, socket)
                  socket.send(JSON.stringify({id: this.id, address: this.address, web: this.web, host: this.host, port: this.port, domain: this.domain, relay, action: 'session'}))
                  this.onServerConnection(socket)
                }
              }
            }
          } else {
            socket.send(JSON.stringify({action: 'error', error: 'route is not supported'}))
            socket.close()
          }
        }
        this.ws.onListening = () => {
          this.emit('start', 'ws')
        }
        this.ws.onClose = () => {
          this.emit('stop', 'ws')
        }
    
        // this.intervalUsage(60000)
    
        this.relay = new DHT()
        this.relay.onListening = () => {
          this.emit('start', 'dht')
        }
        this.relay.onReady = () => {
          this.emit('ev', 'dht is ready')
        }
        this.relay.onError = (err) => {
          this.emit('error', err)
        }
        this.relay.onClose = () => {
          this.emit('stop', 'dht')
        }
        this.relay.onPeer = (peer, infoHash, from) => {
          // if not connected, then connect socket
          // share resource details on websocket
          const ih = infoHash.toString('hex')

          if(this.dev){
            console.log(peer, ih, from)
          }
    
          if(!this.relays.has(ih)){
            return
          }
    
          const useAddress = `${peer.host}:${peer.port}`
          const id = crypto.createHash('sha1').update(useAddress).digest('hex')
          if(self.address === useAddress || self.id === id){
            return
          }
    
          if(this.triedAlready.has(id)){
            const check = this.triedAlready.get(id)
            const checkStamp =  (Date.now() - check.stamp) / 1000
            if(check.wait >= checkStamp){
              return
            }
          }
    
          // if(this.servers.has(id)){
          //   const checkTracker = this.servers.get(id)
          //   const checkRelay = this.relays.get(ih)
          //   if(checkRelay.every((datas) => {return checkTracker.id !== datas.id})){
          //     // checkRelay.push(checkTracker)
          //     // if(!checkTracker.relays.includes(ih)){
          //     //   checkTracker.relays.push(ih)
          //     // }
          //     checkTracker.send(JSON.stringify({action: 'add', relay: ih, reply: true}))
          //   }
          //   return
          // }
    
          if(this.servers.has(id)){
            const checkTracker = this.servers.get(id)
            if(checkTracker.readyState === 1){
              const checkRelay = this.relays.get(ih)
              if(!checkRelay.find((data) => {return checkTracker.id === data.id})){
                checkRelay.push(checkTracker)
                if(!checkTracker.relays.includes(ih)){
                  checkTracker.relays.push(ih)
                }
                checkTracker.send(JSON.stringify({action: 'add', relay: ih, reply: true}))
              }
            }
            return
          }
    
          if(this.limit.serverConnections){
            if(this.relays.get(ih).length < this.limit.serverConnections){
              const relay = `ws://${useAddress}/relay?hash=${ih}&id=${this.id}`
              const con = new WebSocket(relay)
              con.server = false
              con.active = true
              con.relay = ih
              con.relays = []
              con.id = id
              this.servers.set(con.id, con)
              self.onServerConnection(con)
              return
            }
          } else {
            const relay = `ws://${useAddress}/relay?hash=${ih}&id=${this.id}`
            const con = new WebSocket(relay)
            con.server = false
            con.active = true
            con.relay = ih
            con.relays = []
            con.id = id
            this.servers.set(con.id, con)
            self.onServerConnection(con)
            return
          }
        }
        if(this.init){
          this.start()
        }
    }

    // start(){}

    // stop(){}

    onClientConnection (socket) {

      if(this.limit.clientConnections){
        if(this.clients.size > this.limit.clientConnections){
          this.http.close()
        }
      }

      socket.onMessage = (data, buffer) => {
        try {
          data = JSON.parse(data.toString('utf-8'))
          if(this.dev){
            console.log('ws client message', data)
          }
          // if(message.action === 'pong'){
          //   socket.active = true
          // }
          if(data.action === 'session'){
            this.matchOffers(socket)
          } else if(data.action === 'proc'){
            if(this.clients.has(data.res)){
              const test = this.clients.get(data.res)
              test.send(JSON.stringify({action: 'shake'}))
              if(test.ids.has(data.req)){
                test.ids.delete(data.req)
              }
              if(!test.web.has(data.req)){
                test.web.add(data.req)
              }
              if(this.limit.signalConnections && test.web.size >= this.limit.signalConnections){
                test.close()
              }
            }
            if(socket.ids.has(data.res)){
              socket.ids.delete(data.res)
            }
            if(!socket.web.has(data.res)){
              socket.web.add(data.res)
            }
            if(this.limit.signalConnections && socket.web.size >= this.limit.signalConnections){
              socket.close()
            }
          }
          if(data.action === 'request'){
            if(socket.ids.has(data.res) && this.clients.has(data.res)){
              const test = this.clients.get(data.res)
              test.send(JSON.stringify(data))
            } else {
              socket.send(JSON.stringify({action: 'interrupt', id: data.res}))
              this.matchOffers(socket)
            }
          }
          if(data.action === 'response'){
            if(socket.ids.has(data.req) && this.clients.has(data.req)){
              const test = this.clients.get(data.req)
              test.send(JSON.stringify(data))
            } else {
              socket.send(JSON.stringify({action: 'interrupt', id: data.req}))
              this.matchOffers(socket)
            }
          }
        } catch (err) {
          this.emit('error', err)
          socket.close()
        }
      }

      socket.onError = (err) => {
        this.emit('error', err)
      }

      socket.onClose = (code, reason) => {
        socket.onHandle()
        if(this.offers.has(socket.hash)){
          const offer = this.offers.get(socket.hash)
          if(offer.has(socket.id)){
            offer.delete(socket.id)
          }
        }
        socket.ids.forEach((id) => {
          if(this.clients.has(id)){
            const matched = this.clients.get(id)
            matched.send(JSON.stringify({action: 'interrupt', id: socket.id}))
            matched.ids.delete(socket.id)
            this.matchOffers(matched)
          }
        })
        socket.ids.clear()
        socket.web.clear()
        this.clients.delete(socket.id)
        this.emit('ev', `code: ${code} reason: ${reason.toString()}`)
      }

      socket.onHandle = () => {
        socket.off('message', socket.onMessage)
        socket.off('error', socket.onError)
        socket.off('close', socket.onClose)
      }

      socket.on('message', socket.onMessage)
      socket.on('error', socket.onError)
      socket.on('close', socket.onClose)

      this.matchOffers(socket)
    }

    onServerConnection(socket){
      // ifhash sent from messages exists already in this.sockets then close the socket
      socket.onOpen = () => {
        // do limit check
        // send the right messages
        // self.sockets[socket.id] = socket
        if(socket.id){
          if(this.triedAlready.has(socket.id)){
            this.triedAlready.delete(socket.id)
          }
        }
        socket.send(JSON.stringify({id: this.id, address: this.address, web: this.web, host: this.host, port: this.port, domain: this.domain, relay: socket.relay, action: 'session'}))
      }
      socket.onError = (err) => {
        let useSocket
        if(socket.id){
          useSocket = socket.id
          if(this.triedAlready.has(socket.id)){
            const check = this.triedAlready.get(socket.id)
            check.stamp = Date.now()
            check.wait = check.wait * 2
          } else {
            this.triedAlready.set(socket.id, {stamp: Date.now(), wait: 1})
          }
        } else {
          useSocket = 'socket'
        }
        err.msg = useSocket + ' had an error, will wait and try to connect later'
        this.emit('error', err)
      }
      socket.onMessage = (data, buffer) => {
        // do limit check
        // send the right data
        try {
          data = JSON.parse(data.toString('utf-8'))
          if(this.dev){
            console.log('ws server message', data)
          }
          if(data.action === 'session'){
            if(this.relays.has(data.id) || socket.relay !== data.relay || data.id !== crypto.createHash('sha1').update(data.address).digest('hex')){
              socket.close()
              return
            }
            if(!socket.relays.includes(data.relay)){
              socket.relays.push(data.relay)
            }
            // data.relays = [useRelay]
            delete data.relay
            delete socket.relay
            for(const m in data){
              socket[m] = data[m]
            }
            for(const r of socket.relays){
              if(this.relays.has(r)){
                this.relays.get(r).push(socket)
              }
            }
            socket.session = true
          }
          if(data.action === 'add'){
            if(!this.relays.has(data.relay)){
              return
            }
  
            const checkRelay = this.relays.get(data.relay)
            const i = checkRelay.findIndex((datas) => {return socket.id === datas.id})
            if(i === -1){
              checkRelay.push(socket)
            }
  
            if(!socket.relays.includes(data.relay)){
              socket.relays.push(data.relay)
            }
          }
          if(data.action === 'sub'){
            if(!this.relays.has(data.relay)){
              return
            }
            if(socket.relays.length === 1 && socket.relays.includes(data.relay)){
              socket.close()
              return
            }
  
            const checkRelay = this.relays.get(data.relay)
            const i = checkRelay.findIndex((datas) => {return socket.id === datas.id})
            if(i !== -1){
              checkRelay.splice(i, 1)
            }
  
            if(socket.relays.includes(data.relay)){
              socket.relays.splice(socket.relays.indexOf(data.relay), 1)
            }
          }
          if(data.action === 'ping'){
            socket.send(JSON.stringify({action: 'pong'}))
          }
          if(data.action === 'pong'){
            socket.active = true
          }
          if(data.action === 'on'){
            for(const r of socket.relays){
              if(this.relays.has(r)){
                const checkRelay = this.relays.get(r)
                const i = checkRelay.find((soc) => {return socket.id === soc.id})
                if(i){
                  i.session = true
                }
              }
            }
          }
          if(data.action === 'off'){
            for(const r of socket.relays){
              if(this.relays.has(r)){
                const checkRelay = this.relays.get(r)
                const i = checkRelay.find((soc) => {return socket.id === soc.id})
                if(i){
                  i.session = false
                }
              }
            }
          }
        } catch (err) {
          err.msg = socket.id || 'socket' + ' had an error, will wait and try to connect later'
          this.emit('error', err)
          socket.close()
        }
      }
      socket.onClose = (code, reason) => {
        socket.handleListeners()
  
        if(socket.relays){
          for(const soc of socket.relays){
            if(this.relays.has(soc)){
              const checkRelay = this.relays.get(soc)
              const i = checkRelay.findIndex((datas) => {return socket.id === datas.id})
              if(i !== -1){
                checkRelay.splice(i, 1)
              }
            }
          }
        }
  
        if(socket.id){
          if(this.servers.has(socket.id)){
            this.servers.delete(socket.id)
          }
        }
  
        this.emit('ev', `code: ${code} reason: ${reason.toString()}`)
      }

      socket.handleListeners = () => {
        socket.off('open', socket.onOpen)
        socket.off('error', socket.onError)
        socket.off('message', socket.onMessage)
        socket.off('close', socket.onClose)
      }
      
      socket.on('open', socket.onOpen)
      socket.on('error', socket.onError)
      socket.on('message', socket.onMessage)
      socket.on('close', socket.onClose)
    }

    matchOffers(socket){
      const testing = this.offers.has(socket.hash) ? this.offers.get(socket.hash) : null
      if(testing){
        if(testing.has(socket.id)){
          return
        }
        for(const test of testing.values()){
          if(socket.web.has(test) || socket.ids.has(test)){
            continue
          } else {
            if(this.clients.has(test)){
              const chan = this.clients.get(test)
              chan.ids.add(socket.id)
              socket.ids.add(chan.id)
              // socket.send(JSON.stringify({req: socket.id, res: chan.id, action: 'init'}))
              chan.send(JSON.stringify({req: chan.id, res: socket.id, action: 'init'}))
              testing.delete(test)
              return
            } else {
              testing.delete(test)
              continue
            }
          }
        }
      }
      if(testing){
        testing.add(socket.id)
      }
    }
    start(){
      this.relay.on('listening', this.relay.onListening)
      this.relay.on('ready', this.relay.onReady)
      this.relay.on('peer', this.relay.onPeer)
      this.relay.on('error', this.relay.onError)
      this.relay.on('close', this.relay.onClose)
      if(!this.relay.listening){
        this.relay.listen(this.port, this.server)
      }
      this.ws.on('listening', this.ws.onListening)
      this.ws.on('connection', this.ws.onConnection)
      this.ws.on('error', this.ws.onError)
      this.ws.on('close', this.ws.onClose)
      this.http.on('listening', this.http.onListening)
      this.http.on('request', this.http.onRequest)
      this.http.on('error', this.http.onError)
      this.http.on('close', this.http.onClose)
      if(!this.http.listening){
        this.http.listen(this.port, this.server)
      }
      if(!this.checkServer){
        this.checkServer = setInterval(() => {
          for(const test in this.servers.values()){
            if(!test.active){
              test.terminate()
              continue
            } else {
              test.active = false
              test.send(JSON.stringify({action: 'ping'}))
            }
          }
        }, this.timer.checkServer)
      }
      if(!this.checkClient){
        this.checkClient = setInterval(() => {
          for(const test in this.clients.values()){
            if(!test.active){
              test.terminate()
              continue
            } else {
              test.active = false
              test.send(JSON.stringify({action: 'ping'}))
            }
          }
        }, this.timer.checkClient)
      }

      this.talk()

      if(!this.talking){
        this.talking = setInterval(() => {this.talk()}, this.timer.talking)
      }
    }
    stop(){
      this.ws.off('listening', this.ws.onListening)
      this.ws.off('connection', this.ws.onConnection)
      this.ws.off('error', this.ws.onError)
      this.ws.off('close', this.ws.onClose)
      this.http.off('listening', this.http.onListening)
      this.http.off('request', this.http.onRequest)
      this.http.off('error', this.http.onError)
      this.http.off('close', this.http.onClose)
      if(this.http.listening){
        this.http.close()
      }
      this.relay.off('listening', this.relay.onListening)
      this.relay.off('ready', this.relay.onReady)
      this.relay.off('peer', this.relay.onPeer)
      this.relay.off('error', this.relay.onError)
      this.relay.off('close', this.relay.onClose)
      if(this.relay.listening){
        this.relay.destroy()
      }
      if(this.checkServer){
        clearInterval(this.checkServer)
        this.checkServer = null
      }
      if(this.checkClient){
        clearInterval(this.checkClient)
        this.checkClient = null
      }
      if(this.talking){
        clearInterval(this.talking)
        this.talking = null
      }
    }
    talk(){
      for(const test of this.relays.keys()){
        if(this.limit.serverConnections && this.relays.get(test).size >= this.limit.serverConnections){
          continue
        } else {
          this.relay.lookup(test, (err, num) => {
            if(err){
              this.emit('error', err)
            } else {
              this.emit('ev', `${test}: ${num}`)
            }
          })
          this.relay.announce(test, this.port, (err) => {
            if(err){
              this.emit('error', err)
            } else {
              this.emit('ev', `announced: ${test}`)
            }
          })
        }
      }
    }
    randomRelay(hash){
      const test = this.relays.get(crypto.createHash('sha1').update(hash).digest('hex')).filter((e) => {return e.session && e.web})
      return test.length ? test[Math.floor(Math.random() * test.length)].web : null
    }
}