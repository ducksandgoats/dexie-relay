import {Client} from 'relay-to-relay'
import {Dexie} from 'dexie'
import {EventEmitter} from 'events'

export default class Base extends EventEmitter {
    constructor(opts){
        super()
        this._debug = opts.debug

        this._sync = opts.sync === true || opts.sync === null || opts.sync === false ? opts.sync : null

        this._force = opts.force === false ? opts.force : true
    
        this._keep = opts.keep === true ? opts.keep : false

        this._timer = typeof(opts.timer) === 'object' && !Array.isArray(opts.timer) ? opts.timer : {}
        this._timer.redo = this._timer.redo || 180000
        this._timer.expire = this._timer.expire || 300000
    
        this._user = localStorage.getItem('user') || (() => {const test = crypto.randomUUID();localStorage.setItem('user', test);return test;})()
        
        this.client = new Client(opts.url, opts.hash, opts.rtor)

        // this.crud = {}
    
        for(const records in opts.schema){
            const record = opts.schema[records].split(',').map((data) => {return data.replaceAll(' ', '')})
            if(!record.includes('stamp')){
                record.push('stamp')
            }
            if(!record.includes('edit')){
                record.push('edit')
            }
            if(record.includes('iden')){
                record.splice(record.indexOf('iden'), 1)
                record.unshift('iden')
            } else {
                record.unshift('iden')
            }
            opts.schema[records] = record.join(',')

            // this.crud[records] = {table: () => {return tables(records)}, clear: async () => {return await clears(records)}, add: async (data) => {return await adds(records, data)}, sub: async (prop) => {return await subs(records, prop)}, ret: async (prop) => {return await rets(records, prop)}, edit: async (prop, data) => {return await edits(records, prop, data)}}
        }
        
        this.db = new Dexie(opts.name, {})
        if(this._debug){
            console.log('name', this.db.name)
        }
        this.db.version(opts.version).stores(opts.schema)

        // const rets = async (name, prop) => {
        //     const dataTab = this.db.table(name)
        //     return await dataTab.get(prop)
        // }

        // const adds = async (name, data) => {
        //     const dataTab = this.db.table(name)
        //     data.stamp = data.stamp || Date.now()
        //     data.user = data.user || this._user
        //     data.iden = data.iden || crypto.randomUUID()
        //     data.edit = 0
        //     const test = await dataTab.add(data)
        //     // this.emit('add', test)
        //     this.client.onSend(JSON.stringify({name, data, user: data.user, stamp: data.stamp, iden: test, status: 'add'}))
        //     return test
        // }

        // const edits = async (name, prop, data) => {
        //     const dataTab = this.db.table(name)
        //     const test = await dataTab.get(prop)
        //     if((test && test.user === this._user) && (!data.user || data.user === this._user)){
        //         data.edit = Date.now()
        //         const num = await dataTab.update(prop, data)
        //         // this.emit('edit', test.iden)
        //         this.client.onSend(JSON.stringify({name, data, iden: test.iden, user: test.user, edit: data.edit, num, status: 'edit'}))
        //         return test.iden
        //     } else {
        //         throw new Error('user does not match')
        //     }
        // }

        // const subs = async (name, prop) => {
        //     const dataTab = this.db.table(name)
        //     const test = await dataTab.get(prop)
        //     if(!test){
        //         throw new Error('did not find data')
        //     }
        //     if(this._force){
        //         await dataTab.delete(prop)
        //         // this.emit('sub', test.iden)
        //         if(test.user === this._user){
        //             this.client.onSend(JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'}))
        //         }
        //         return test.iden
        //     } else {
        //         if(test.user === this._user){
        //             await dataTab.delete(prop)
        //             // this.emit('sub', test.iden)
        //             this.client.onSend(JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'}))
        //             return test.iden
        //         } else {
        //             throw new Error('user does not match')
        //         }
        //     }
        // }

        // const clears = async (name) => {
        //     const dataTab = this.db.table(name)
        //     await dataTab.clear()
        // }

        // const tables = (name) => {
        //     return this.db.table(name)
        // }

        this._routine = setInterval(() => {
            this._adds.clear()
            for(const [prop, update] of this._edits.entries()){
                if((Date.now() - update) > this._timer.expire){
                    this._edits.delete(prop)
                }
            }
            this._subs.clear()
        }, this._timer.redo)

        this._adds = new Set()
        this._edits = new Map()
        this._subs = new Set()

        this._message = async (data, nick) => {
            try {
                if(this._debug){
                    console.log('Received Message: ', typeof(data), data)
                }
    
                const datas = JSON.parse(data)
    
                const dataTab = this.db.table(datas.name)
    
                if(datas.status){
                    if(datas.user === this._user){
                        return
                    }
                    if(datas.status === 'add'){
                        if(this._adds.has(datas.iden)){
                            return
                        }
                        await dataTab.add(datas.data)
                        this.emit('add', datas.iden)
                        this._adds.add(datas.iden)
                        this.client.onMesh(data, nick)
                    } else if(datas.status === 'edit'){
                        if(this._edits.has(datas.iden)){
                            const test = this._edits.get(datas.iden)
                            if(datas.edit > test){
                                await dataTab.update(datas.iden, datas.data)
                                this.emit('edit', datas.iden)
                                this._edits.set(datas.iden, datas.edit)
                                this.client.onMesh(data, nick)
                            } else {
                                return
                            }
                        } else {
                            await dataTab.update(datas.iden, datas.data)
                            this.emit('edit', datas.iden)
                            this._edits.set(datas.iden, datas.edit)
                            this.client.onMesh(data, nick)
                        }
                    } else if(datas.status === 'sub'){
                        if(this._subs.has(datas.iden)){
                            return
                        }
                        if(!this._keep){
                            await dataTab.delete(datas.iden)
                            this.emit('sub', datas.iden)
                        }
                        this._subs.add(datas.iden)
                        this.client.onMesh(data, nick)
                    } else {
                        return
                    }
                } else if(datas.session){
                    if(datas.session === 'request'){
                        const stamps = await dataTab.where('stamp').notEqual(0).toArray()
                        while(stamps.length){
                            datas.session = 'response'
                            datas.edits = null
                            datas.stamps = stamps.splice(stamps.length - 50, 50)
                            this.client.onSend(JSON.stringify(datas), nick)
                        }
                        const edits = await dataTab.where('edit').notEqual(0).toArray()
                        while(edits.length){
                            datas.session = 'response'
                            datas.stamps = null
                            datas.edits = edits.splice(edits.length - 50, 50)
                            this.client.onSend(JSON.stringify(datas), nick)
                        }
                    } else if(datas.session === 'response'){
                        if(datas.stamps){
                            if(!datas.stamps.length){
                                return
                            }
                            const arr = []
                            for(const data of datas.stamps){
                                try {
                                    await dataTab.add(data)
                                    arr.push(data.iden)
                                } catch (err) {
                                    if(this._debug){
                                        console.error(err)
                                    }
                                    continue
                                }
                            }
                            this.emit('bulk', arr)
                        }
                        if(datas.edits){
                            if(!datas.edits.length){
                                return
                            }
                            const arr = []
                            for(const data of datas.edits){
                                try {
                                    const got = await dataTab.get(data.iden)
                                    if(got){
                                        if(got.edit < data.edit){
                                            await dataTab.put(data)
                                            arr.push(data.iden)
                                        }
                                    } else {
                                        await dataTab.add(data)
                                        arr.push(data.iden)
                                    }
                                } catch (err) {
                                    if(this._debug){
                                        console.error(err)
                                    }
                                    continue
                                }
                            }
                            this.emit('bulk', arr)
                        }
                    } else if(datas.session === 'stamp'){
                        let stamps
                        if(datas.from && datas.to){
                            stamps = await dataTab.where('stamp').between(datas.from, datas.to, true, true).toArray()
                        } else if(datas.from){
                            stamps = await dataTab.where('stamp').above(datas.from).toArray()
                        } else if(datas.to){
                            stamps = await dataTab.where('stamp').below(datas.to).toArray()
                        } else {
                            return
                        }
                        while(stamps.length){
                            datas.session = 'stamps'
                            datas.stamps = stamps.splice(stamps.length - 50, 50)
                            datas.edits = null
                            this.client.onSend(JSON.stringify(datas), nick)
                        }
                    } else if(datas.session === 'stamps'){
                        if(!datas.stamps.length){
                            return
                        }
                        const arr = []
                        for(const data of datas.stamps){
                            try {
                                await dataTab.add(data)
                                arr.push(data.iden)
                            } catch (err) {
                                if(this._debug){
                                    console.error(err)
                                }
                                continue
                            }
                        }
                        this.emit('bulk', arr)
                    } else if(datas.session === 'edit'){
                        let edits
                        if(datas.from && datas.to){
                            edits = await dataTab.where('stamp').between(datas.from, datas.to, true, true).toArray()
                        } else if(datas.from){
                            edits = await dataTab.where('stamp').above(datas.from).toArray()
                        } else if(datas.to){
                            edits = await dataTab.where('stamp').below(datas.to).toArray()
                        } else {
                            return
                        }
                        while(edits.length){
                            datas.session = 'edits'
                            datas.stamps = null
                            datas.edits = edits.splice(edits.length - 50, 50)
                            this.client.onSend(JSON.stringify(datas), nick)
                        }
                    } else if(datas.session === 'edits'){
                        if(!datas.edits.length){
                            return
                        }
                        const arr = []
                        for(const data of datas.edits){
                            try {
                                const got = await dataTab.get(data.iden)
                                if(got){
                                    if(got.edit < data.edit){
                                        await dataTab.put(data)
                                        arr.push(data.iden)
                                    }
                                } else {
                                    await dataTab.add(data)
                                    arr.push(data.iden)
                                }
                            } catch (err) {
                                if(this._debug){
                                    console.error(err)
                                }
                                continue
                            }
                        }
                        this.emit('bulk', arr)
                    } else {
                        return
                    }
                } else {
                    return
                }
            } catch (err) {
                if(this._debug){
                    console.error(err)
                }
                return
            }
        }
        this._disconnect = (chan) => {
            console.log('disconnected: ' + chan)
        }
        this._err = (e, chan) => {
            console.error(e, chan)
        }
        this._connect = (chan) => {
            console.log('connected: ' + chan)
    
            this.db.tables.forEach(async (table) => {
                let useStamp
                let useEdit
                if(this._sync === true){
                    this.client.onSend(JSON.stringify({name: table.name, session: 'request'}), chan)
                }
                if(this._sync === null){
                    try {
                        useStamp = await table.where('stamp').notEqual(0).last()
                    } catch (err) {
                        if(this._debug){
                            console.error(err)
                        }
                        useStamp = {}
                    }
                    if(useStamp?.stamp){
                        this.client.onSend(JSON.stringify({name: table.name, from: useStamp.stamp, session: 'stamp'}), chan)
                    }
                    try {
                        useEdit = await table.where('edit').notEqual(0).last()
                    } catch (err) {
                        if(this._debug){
                            console.error(err)
                        }
                        useEdit = {}
                    }
                    if(useEdit?.edit){
                        this.client.onSend(JSON.stringify({name: table.name, from: useEdit.edit, session: 'edit'}), chan)
                    }
                }
                if(this._sync === false){
                    if(this._debug){
                        console.log('not syncing')
                    }
                }
            })
        }

        this.client.on('connect', this._connect)
        this.client.on('error', this._err)
        this.client.on('disconnect', this._disconnect)
        this.client.on('message', this._message)
    }

    id(){return crypto.randomUUID()}

    async ret(name, prop){
        const dataTab = this.db.table(name)
        return await dataTab.get(prop)
    }

    async add(name, data){
        const dataTab = this.db.table(name)
        data.stamp = data.stamp || Date.now()
        data.user = data.user || this._user
        data.iden = data.iden || crypto.randomUUID()
        data.edit = 0
        const test = await dataTab.add(data)
        // this.emit('add', test)
        this.client.onSend(JSON.stringify({name, data, user: data.user, stamp: data.stamp, iden: test, status: 'add'}))
        return test
    }

    async edit(name, prop, data){
        const dataTab = this.db.table(name)
        const test = await dataTab.get(prop)
        if((test && test.user === this._user) && (!data.user || data.user === this._user)){
            data.edit = Date.now()
            const num = await dataTab.update(prop, data)
            // this.emit('edit', test.iden)
            this.client.onSend(JSON.stringify({name, data, iden: test.iden, user: test.user, edit: data.edit, num, status: 'edit'}))
            return test.iden
        } else {
            throw new Error('user does not match')
        }
    }

    async sub(name, prop){
        const dataTab = this.db.table(name)
        const test = await dataTab.get(prop)
        if(!test){
            throw new Error('did not find data')
        }
        if(this._force){
            await dataTab.delete(prop)
            // this.emit('sub', test.iden)
            if(test.user === this._user){
                this.client.onSend(JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'}))
            }
            return test.iden
        } else {
            if(test.user === this._user){
                await dataTab.delete(prop)
                // this.emit('sub', test.iden)
                this.client.onSend(JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'}))
                return test.iden
            } else {
                throw new Error('user does not match')
            }
        }
    }

    async clear(name){
        const dataTab = this.db.table(name)
        await dataTab.clear()
    }

    table(name){
        return this.db.table(name)
    }

    quit(){
        clearInterval(this._routine)
        this._adds.clear()
        this._edits.clear()
        this._subs.clear()
        this.client.off('connect', this._connect)
        this.client.off('error', this._err)
        this.client.off('message', this._message)
        this.client.off('disconnect', this._disconnect)
        this.client.end()
        this.db.close()
    }
}