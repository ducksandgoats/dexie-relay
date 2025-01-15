import Channel from 'simple-peer'
import Events from 'events'
import {Level} from 'level'

export default class Client extends Events {
    constructor(url, hash, opts = {}){
        super()
        this.dev = Boolean(opts.dev)
        this.user = Boolean(opts.user)
        this.id = this.user ? localStorage.getItem('id') : sessionStorage.getItem('id')
        this.db = new Level(opts.db || 'relay')
        if(!this.id){
            this.id = crypto.randomUUID()
            if(this.user){
                localStorage.setItem('id', this.id)
            } else {
                sessionStorage.setItem('id', this.id)
            }
        }
        this.simple = opts.simple && typeof(opts.simple) === 'object' && !Array.isArray(opts.simple) ? opts.simple : {}
        this.hash = hash
        this.url = url
        this.channels = new Map()
        this.socket = null
        this.temps = new Map()
        this.routine = null
        this.status = true
        this.auto = opts.auto === false ? opts.auto : true
        if(this.auto){
            this.ws(true)
        }
    }
    begin(){
        this.status = true
        this.ws(true)
    }
    end(){
        this.status = false
        if(this.routine){
            clearTimeout(this.routine)
        }
        if(this.socket){
            this.socket.close()
        }
        this.temps.forEach((data) => {
            if(this.channels.has(data.relay)){
                this.channels.get(data.relay).send(JSON.stringify({...data, action: 'abort'}))
            }
        })
        this.temps.clear()
        this.channels.forEach((data) => {
            data.destroy()
        })
        this.channels.clear()
    }
    ws(amount){
        const test = this.temps.size + this.channels.size
        if(!(test < 6)){
            return
        }
        if(this.socket){
            if(this.socket.readyState === WebSocket.OPEN){
                if(amount){
                    const testing = 6 - test
                    for(let i = 0;i < testing;i++){
                        this.socket.send(JSON.stringify({action: 'session'}))
                    }
                } else {
                    this.socket.send(JSON.stringify({action: 'session'}))
                }
            } else {
                if(!this.routine){
                    setTimeout(() => {this.ws(amount)}, 5000)
                }
            }
            return
        }
        this.socket = new WebSocket(`ws://${this.url}/signal?hash=${this.hash}&id=${this.id}`)
        this.socket.handleOpen = (e) => {
            if(this.dev){
                console.log('websocket connected')
            }
            if(amount){
                const testing = 6 - test
                for(let i = 0;i < testing;i++){
                    this.socket.send(JSON.stringify({action: 'session'}))
                }
            } else {
                this.socket.send(JSON.stringify({action: 'session'}))
            }
            this.emit('open', e)
        }
        this.socket.handleMessage = (e) => {
            try {
                const message = JSON.parse(e.data)
                if(this.dev){
                    console.log('websocket data', typeof(message), message)
                }
                if(message.action === 'error'){
                    this.emit('error', message.error)
                }
                if(message.action === 'relay'){
                    if(message.relay){
                        this.url = message.relay
                        this.socket.relay = true
                        this.socket.close()
                    }
                }
                if(message.action === 'interrupt'){
                    if(this.channels.has(message.id)){
                        const testChannel = this.channels.get(message.id)
                        testChannel.destroy()
                        this.channels.delete(message.id)
                    }
                }
                if(message.action === 'init'){
                    const testChannel = new Channel({...this.simple, initiator: true, trickle: false})
                    new Promise((res) => {testChannel.once('signal', res)})
                    .then((data) => {
                        testChannel.id = message.res
                        testChannel.redo = true
                        testChannel.channels = new Set()
                        testChannel.messages = new Set()
                        testChannel.ws = true
                        if(!this.channels.has(testChannel.id)){
                            this.channels.set(testChannel.id, testChannel)
                        }
                        this.socket.send(JSON.stringify({...message, action: 'request', request: data}))
                        this.handleChannel(testChannel)
                    })
                    .catch((err) => {
                        testChannel.destroy()
                        console.error(err)
                    })
                }
                if(message.action === 'request'){
                    const testChannel = new Channel({...this.simple, initiator: false, trickle: false})
                    new Promise((res) => {testChannel.once('signal', res)})
                    .then((data) => {
                        testChannel.id = message.req
                        testChannel.redo = true
                        testChannel.channels = new Set()
                        testChannel.messages = new Set()
                        testChannel.ws = true
                        if(!this.channels.has(testChannel.id)){
                            this.channels.set(testChannel.id, testChannel)
                        }
                        delete message.request
                        this.socket.send(JSON.stringify({...message, action: 'response', response: data}))
                        this.handleChannel(testChannel)
                    })
                    .catch((err) => {
                        testChannel.destroy()
                        console.error(err)
                    })
                    testChannel.signal(message.request)
                }
                if(message.action === 'response'){
                    if(this.channels.has(message.res)){
                        const testChannel = this.channels.get(message.res)
                        testChannel.signal(message.response)
                        delete message.response
                        this.socket.send(JSON.stringify({...message, action: 'proc'}))
                    }
                }
            } catch (error) {
                this.emit('error', error)
                return
            }
        }
        this.socket.handleError = (e) => {
            this.emit('error', e)
        }
        this.socket.handleClose = (e) => {
            if(this.dev){
                console.log('websocket disconnected')
            }
            this.emit('close', e)
            this.socket.handleEvent()
            if(this.socket.relay){
                if(!this.routine){
                    setTimeout(() => {this.ws(amount)}, 5000)
                }
            }
            delete this.socket
        }
        this.socket.handleEvent = () => {
            this.socket.removeEventListener('open', this.socket.handleOpen)
            this.socket.removeEventListener('message', this.socket.handleMessage)
            this.socket.removeEventListener('error', this.socket.handleError)
            this.socket.removeEventListener('close', this.socket.handleClose)
        }
        this.socket.addEventListener('open', this.socket.handleOpen)
        this.socket.addEventListener('message', this.socket.handleMessage)
        this.socket.addEventListener('error', this.socket.handleError)
        this.socket.addEventListener('close', this.socket.handleClose)
    }
    handleChannel(channel){
        const onConnect = () => {
            if(this.dev){
                console.log('webrtc connect', channel.id)
            }
            
            if(channel.takeOut){
                clearTimeout(channel.takeOut)
                delete channel.takeOut
            }

            if(!channel.ws && channel.msg){
                if(this.channels.has(channel.msg.relay)){
                    this.channels.get(channel.msg.relay).send({action: 'afterSession', id: channel.msg.id})
                }
                delete channel.msg
            }

            this.channels.forEach((data) => {
                if(data.id !== channel.id){
                    if(data.connected){
                        data.send(`trystereo:${JSON.stringify({action: 'add', add: channel.id})}`)
                    }
                    channel.send(`trystereo:${JSON.stringify({action: 'add', add: data.id})}`)
                }
            })
            this.emit('connect', channel.id)
        }
        const onData = async (data) => {
            try {
                data = new TextDecoder().decode(data)
                if(this.dev){
                    console.log('webrtc data', typeof(data), data)
                }
                if(data.startsWith('trystereo:')){
                    data = JSON.parse(data.replace('trystereo:', ''))
                    if(data.action === 'add'){
                        if(!channel.channels.has(data.add)){
                            channel.channels.add(data.add)
                        }
                    } else if(data.action === 'sub'){
                        if(!channel.channels.has(data.sub)){
                            channel.channels.add(data.sub)
                        }
                    } else if(data.action === 'beforeSearch'){
                        await this.beforeSearch(data, channel)
                    } else if(data.action === 'afterSearch'){
                        await this.afterSearch(data, channel)
                    } else if(data.action === 'beforeSession'){
                        await this.beforeSession(data, channel)
                    } else if(data.action === 'afterSession'){
                        await this.afterSession(data, channel)
                    } else if(data.action === 'nonmsg'){
                        await this.nonmsg(data)
                    } else if(data.action === 'abort'){
                        await this.abortion(data, channel)
                    } else {
                        this.emit('error', new Error('data is invalid'))
                    }
                } else {
                    this.emit('message', data, channel.id)
                }
            } catch (error) {
                this.emit('error', error)
                return
            }
        }
        const onError = (err) => {
            if(this.dev){
                console.error('webrtc error', err)
            }
            this.emit('error', err, channel.id)
            // if(!channel.connected){
            //     channel.destroy()
            // }
        }
        const onClose = () => {
            if(this.dev){
                console.log('webrtc data', channel.id)
            }
            onHandle()

            if(channel.takeOut){
                clearTimeout(channel.takeOut)
                delete channel.takeOut
            }

            channel.messages.forEach(async (data) => {
                const test = await this.dbGet(data)
                if(test){
                    if(test.startRelay){
                        if(this.channels.has(test.startRelay)){
                            this.channels.get(test.startRelay).send(JSON.stringify({...test, action: 'abort'}))
                        }
                    }
                    if(test.stopRelay){
                        if(this.channels.has(test.stopRelay)){
                            this.channels.get(test.stopRelay).send(JSON.stringify({...test, action: 'abort'}))
                        }
                    }
                }
                channel.messages.delete(data)
            })
            channel.messages.clear()

            this.channels.forEach((chan) => {
                if(chan.id !== channel.id && chan.connected){
                    chan.send(`trystereo:${JSON.stringify({action: 'sub', sub: channel.id})}`)
                }
            })
            if(this.channels.has(channel.id)){
                this.channels.delete(channel.id)
            }
            if(this.status){
                this.freshRelay(channel)
            }
            this.emit('disconnect', channel.id)
            // channel.emit('disconnected', channel)
        }
        const onHandle = () => {
            channel.off('connect', onConnect)
            channel.off('data', onData)
            // channel.off('stream', onStream)
            // channel.off('track', onTrack)
            channel.off('error', onError)
            channel.off('close', onClose)
        }
        channel.on('connect', onConnect)
        channel.on('data', onData)
        channel.on('error', onError)
        channel.on('close', onClose)
    }
    onSend(data, id = null){
        if(id){
            if(this.channels.has(id)){
                const test = this.channels.get(id)
                if(test.connected){
                    test.send(data)
                }
            }
        } else {
            this.channels.forEach((prop) => {
                if(prop.connected){
                    prop.send(data)
                }
            })
        }
    }
    onMesh(data, id){
        if(this.channels.has(id)){
            const chans = this.channels.get(id)
            for(const chan of this.channels.values()){
                if(chan.connected && chans.id !== chan.id){
                    if(!chan.channels.has(chans.id)){
                        const test = chans.channels.intersection(chan.channels)
                        if(test.size){
                            let i = true
                            for(const prop of test.values()){
                                if(this.id > prop){
                                    i = false
                                    break
                                }
                            }
                            if(i){
                                chan.send(data)
                            }
                        } else {
                            chan.send(data)
                        }
                    }
                }
            }
        }
    }
    async abortion(obj, chan){
        const test = await this.dbGet(obj.id)
        if(test){
            if(chan.id === test.startRelay && test.stopRelay && this.channels.has(test.stopRelay)){
                this.channels.get(test.stopRelay).send(JSON.stringify(obj))
            }
            if(chan.id === test.stopRelay && test.startRelay && this.channels.has(test.startRelay)){
                this.channels.get(test.startRelay).send(JSON.stringify(obj))
            }
            await this.dbDelete(test.id)
        } else {
            if(this.temps.has(obj.id)){
                this.temps.delete(obj.id)
                if((this.temps.size + this.channels.size) < 6){
                    this.rtc()
                }
            } else if(this.id === obj.start){
                if(this.channels.has(obj.stop)){
                    const testChan = this.channels.get(obj.stop)
                    if(testChan.msg){
                        delete testChan.msg
                    }
                    if(!testChan.connected){
                        testChan.destroy()
                    }
                }
            } else if(this.id === obj.stop){
                if(this.channels.has(obj.start)){
                    const testChan = this.channels.get(obj.start)
                    if(testChan.msg){
                        delete testChan.msg
                    }
                    if(!testChan.connected){
                        testChan.destroy()
                    }
                }
            } else {
                return
            }
        }
    }
    async beforeSearch(obj, chan){
        if(this.id === obj.start || this.temps.has(obj.id)){
            obj.action = 'nonmsg'
            chan.send('trystereo:' + JSON.stringify(obj))
            return
        } else if((this.channels.size + this.temps.size) < 6 && !this.channels.has(obj.start)){
            const testChannel = new Channel({...this.simple, initiator: true, trickle: false})
            new Promise((res) => {testChannel.once('signal', res)})
            .then((data) => {
                testChannel.id = obj.start
                testChannel.redo = true
                testChannel.ws = false
                testChannel.msg = {id: obj.id, relay: chan.id, start: obj.start, stop: this.id}
                testChannel.channels = new Set()
                testChannel.messages = new Set()
                // if(!this.channels.has(testChannel.id)){
                this.channels.set(testChannel.id, testChannel)
                // }
                // delete obj.start
                chan.send(JSON.stringify({...obj, action: 'afterSearch', data, stop: this.id}))
                testChannel.takeOut = setTimeout(() => {
                    testChannel.redo = false
                    testChannel.destroy()
                }, 60000)
                this.handleChannel(testChannel)
            })
            .catch((err) => {
                testChannel.destroy()
                chan.send(JSON.stringify({...obj, action: 'abort'}))
                console.error(err)
            })
        } else {
            const test = await this.dbGet(obj.id)
            if(test){
                obj.action = 'nonmsg'
                chan.send('trystereo:' + JSON.stringify(obj))
                return
            } else {
                if(!chan.messages.has(obj.id)){
                    chan.messages.add(obj.id)
                }
            }
            const base = {startRelay: chan.id, tried: [], id: obj.id, start: obj.start}
            const checkInit = await this.dbPost(base.id, base)
            if(!checkInit){
                await this.dbDelete(base.id)
                obj.action = 'nonmsg'
                chan.send('trystereo:' + JSON.stringify(obj))
                return
            }

            const arr = []
            const list = new Set()
            for(const prop of this.channels.values()){
                arr.push(prop)
                list.add(prop.id)
            }
            const notTried = arr.filter((data) => {return !base.tried.includes(data.id) && data.connected && data.id !== base.startRelay && list.difference(data.channels).size})
            if(this.dev){
                console.log(notTried)
            }
            const i = notTried[Math.floor(Math.random() * notTried.length)]
            if(i){
                obj.action = 'beforeSearch'
                base.tried.push(i.id)
                base.stopRelay = i.id
                const checkBefore = this.dbPost(base.id, base)
                if(!checkBefore){
                    await this.dbDelete(base.id)
                    obj.action = 'nonmsg'
                    chan.send('trystereo:' + JSON.stringify(obj))
                    return
                }
                i.send('trystereo:' + JSON.stringify(obj))
            } else {
                if(this.channels.has(base.startRelay)){
                    obj.action = 'nonmsg'
                    const testChan = this.channels.get(base.startRelay)
                    testChan.send('trystereo:' + JSON.stringify(obj))
                    if(testChan.messages.has(base.id)){
                        testChan.messages.delete(base.id)
                    }
                }
                if(this.dev){
                    console.log('deleted db obj')
                }
                await this.dbDelete(base.id)
            }
        }
    }
    async nonmsg(obj){
        if(this.temps.has(obj.id)){
            const base = this.temps.get(obj.id)
            const arr = []
            const list = new Set()
            for(const prop of this.channels.values()){
                arr.push(prop)
                list.add(prop.id)
            }
            const notTried = arr.filter((data) => {return !base.tried.includes(data.id) && data.connected && list.difference(data.channels).size})
            if(this.dev){
                console.log(notTried)
            }
            const i = notTried[Math.floor(Math.random() * notTried.length)]
            if(i){
                obj.action = 'beforeSearch'
                base.tried.push(i.id)
                base.relay = i.id
                i.send('trystereo:' + JSON.stringify(obj))
            } else {
                base.destroy()
                return
            }
        } else {
            const base = await this.dbGet(obj.id)
            if(!base){
                return
            } else {
                if(!this.channels.has(base.startRelay)){
                    await this.dbDelete(base.id)
                    return
                }
            }
    
            const arr = []
            const list = new Set()
            for(const prop of this.channels.values()){
                arr.push(prop)
                list.add(prop.id)
            }
            const notTried = arr.filter((data) => {return !base.tried.includes(data.id) && data.connected && data.id !== base.startRelay && list.difference(data.channels).size})
            if(this.dev){
                console.log(notTried)
            }
            const i = notTried[Math.floor(Math.random() * notTried.length)]
            if(i){
                obj.action = 'beforeSearch'
                base.tried.push(i.id)
                base.stopRelay = i.id
                const checkBefore = await this.dbPost(base.id, base)
                if(!checkBefore){
                    await this.dbDelete(base.id, base)
                    obj.action = 'nonmsg'
                    const sendToChannel = this.channels.has(base.startRelay) ? this.channels.get(base.startRelay) : null
                    if(sendToChannel){
                        sendToChannel.send('trystereo:' + JSON.stringify(obj))
                    }
                    return
                }
                i.send('trystereo:' + JSON.stringify(obj))
            } else {
                if(this.channels.has(base.startRelay)){
                    obj.action = 'nonmsg'
                    const sendToChannel = this.channels.get(base.startRelay)
                    sendToChannel.send('trystereo:' + JSON.stringify(obj))
                    if(sendToChannel.messages.has(base.id)){
                        sendToChannel.messages.delete(base.id)
                    }
                }
                if(this.dev){
                    console.log('deleted db obj')
                }
                await this.dbDelete(base.id)
            }
        }
    }
    async afterSearch(obj, chan){
        if(this.id === obj.start && this.temps.has(obj.id)){
            const tempChannel = this.temps.get(obj.id)
            if(tempChannel.relay !== chan.id){
                this.temps.delete(tempChannel.id)
                chan.send(JSON.stringify({...obj, action: 'abort'}))
                return
            }
            const testChannel = new Channel({...this.simple, initiator: false, trickle: false})
            new Promise((res) => {testChannel.once('signal', res)})
            .then(async (data) => {
                testChannel.id = obj.stop
                testChannel.msg = tempChannel
                testChannel.msg.stop = obj.stop
                this.temps.delete(tempChannel.id)
                testChannel.redo = true
                testChannel.ws = false
                testChannel.channels = new Set()
                testChannel.messages = new Set()
                if(!this.channels.has(testChannel.id)){
                    this.channels.set(testChannel.id, testChannel)
                }
                delete obj.data
                chan.send(JSON.stringify({...obj, action: 'beforeSession', data}))
                testChannel.takeOut = setTimeout(() => {
                    tempChannel.redo = false
                    testChannel.destroy()
                }, 60000)
                this.handleChannel(testChannel)
            })
            .catch((err) => {
                this.emit('error', err)
                testChannel.destroy()
                this.temps.delete(tempChannel.id)
                chan.send(JSON.stringify({...obj, action: 'abort'}))
                console.error(err)
            })
            testChannel.signal(obj.data)
        } else {
            const test = await this.dbGet(obj.id)
            if(test){
                // test.stopRelay === chan.id && test.start === obj.start && this.channels.has(test.startRelay)
                if(test.stopRelay === chan.id && this.channels.has(test.startRelay)){
                    test.stop = obj.stop
                    this.channels.get(test.startRelay).send(JSON.stringify(obj))
                    if(!chan.messages.has(obj.id)){
                        chan.messages.add(obj.id)
                    }
                    const afterCheck = await this.dbPost(test.id, test)
                    if(!afterCheck){
                        await this.dbDelete(test.id)
                        obj.action = 'abort'
                        chan.send('trystereo:' + JSON.stringify(obj))
                    }
                } else {
                    obj.action = 'abort'
                    chan.send('trystereo:' + JSON.stringify(obj))
                    await this.dbDelete(test.id)
                }
            } else {
                obj.action = 'abort'
                chan.send('trystereo:' + JSON.stringify(obj))
            }
        }
    }
    rtc(){
        const test = {id: crypto.randomUUID(), tried: [], start: this.id}
        test.destroy = () => {
            if(this.temps.has(test.id)){
                this.temps.delete(test.id)
            }
            if(this.status){
                this.freshTemp(test)
            }
        }
        this.temps.set(test.id, test)

        const arr = []
        const list = new Set()
        for(const prop of this.channels.values()){
            arr.push(prop)
            list.add(prop.id)
        }
        const notTried = arr.filter((data) => {return !test.tried.includes(data.id) && data.connected && list.difference(data.channels).size})
        if(this.dev){
            console.log(notTried)
        }
        const i = notTried[Math.floor(Math.random() * notTried.length)]
        if(i){
            const obj = {id: test.id, start: test.start, action: 'beforeSearch'}
            test.tried.push(i.id)
            test.relay = i.id
            i.send('trystereo:' + JSON.stringify(obj))
        } else {
            this.temps.delete(test.id)
            if(this.dev){
                console.log('deleted temp obj')
            }
            return
        }
    }
    async beforeSession(obj, chan){
        if(this.channels.has(obj.start)){
            const testChannel = this.channels.get(obj.start)
            if(testChannel.msg.relay !== chan.id){
                testChannel.destroy()
                chan.send(JSON.stringify({...obj, action: 'abort'}))
                return
            }
            testChannel.signal(obj.data)
            delete obj.data
        } else {
            const test = await this.dbGet(obj.id)
            if(test){
                // chan.id === test.startRelay && test.start === obj.start && test.stop === obj.stop && this.channels.has(test.stopRelay)
                if(chan.id === test.startRelay && this.channels.has(test.stopRelay)){
                    this.channels.get(test.stopRelay).send(JSON.stringify(obj))
                    if(!chan.messages.has(obj.id)){
                        chan.messages.add(obj.id)
                    }
                    const afterCheck = await this.dbPost(test.id, test)
                    if(!afterCheck){
                        await this.dbDelete(test.id)
                        obj.action = 'abort'
                        chan.send('trystereo:' + JSON.stringify(obj))
                    }
                } else {
                    obj.action = 'abort'
                    chan.send('trystereo:' + JSON.stringify(obj))
                    await this.dbDelete(test.id)
                }
            } else {
                obj.action = 'abort'
                chan.send('trystereo:' + JSON.stringify(obj))
            }
        }
    }
    async afterSession(obj, chan){
        const base = await this.dbGet(obj.id)
        if(base){
            if(base.startRelay === chan.id){
                if(this.channels.has(base.stopRelay)){
                    this.channels.get(base.stopRelay).send(JSON.stringify(obj))
                }
            }
            if(base.stopRelay === chan.id){
                if(this.channels.has(base.startRelay)){
                    this.channels.get(base.startRelay).send(JSON.stringify(obj))
                }
            }
            await this.dbDelete(obj.id)
        } else {
            if(obj.start === this.id){
                if(this.channels.has(obj.stop)){
                    const test = this.channels.get(obj.stop)
                    delete test.msg
                }
            }
            if(obj.stop === this.id){
                if(this.channels.has(obj.start)){
                    const test = this.channels.get(obj.start)
                    delete test.msg
                }
            }
        }
    }

    freshRelay(channel){
        const count = this.temps.size + this.channels.size
        if(count < 1){
            this.ws(true)
        } else if(count < 3){
            if(!channel.ws){
                if(channel.msg){
                    if(this.channels.has(channel.msg.relay)){
                        this.channels.get(channel.msg.relay).send(JSON.stringify({...channel.msg, action: 'abort'}))
                    }
                    delete channel.msg
                }
            }
            this.ws(false)
        } else if(count < 6){
            this.rtc()
        } else {
            if(this.dev){
                console.log('have 6 or more users')
            }
        }
    }

    freshTemp(base){
        // this.temps.delete(base.id)
        const count = this.temps.size + this.channels.size
        if(count < 1){
            this.ws(true)
        } else if(count < 3){
            this.ws(false)
        } else if(count < 6){
            this.rtc()
        } else {
            if(this.dev){
                console.log(base.id, 'have 6 or more users')
            }
        }
    }

    async dbDelete(id){
        try {
            await this.db.del(id)
            return true
        } catch {
            return false
        }
    }

    async dbPost(id, data){
        try {
            await this.db.put(id, data)
            return true
        } catch {
            return false
        }
    }

    async dbGet(id){
        try {
            return await this.db.get(id)
        } catch {
            return null
        }
    }

    tracks(){
        return {total: this.channels.size + this.temps.size, temp: this.temps.size, conn: this.channels.size}
    }
}