import {Client} from 'relay-to-relay'
import {Dexie} from 'dexie'
import {EventEmitter} from 'events'

export default class Base extends EventEmitter {
    constructor(opts){
        super()
        this._debug = opts.debug

        this._force = opts.force === false ? opts.force : true
    
        this._keep = opts.keep === true ? opts.keep : false

        this._timer = typeof(opts.timer) === 'object' && !Array.isArray(opts.timer) ? opts.timer : {}
        this._timer.redo = this._timer.redo || 180000
        this._timer.expire = this._timer.expire || 300000
    
        this._user = localStorage.getItem('user') || (() => {const test = crypto.randomUUID();localStorage.setItem('user', test);return test;})()
        
        this.client = new Client(opts.url, opts.hash, opts.rtor)
    
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
        }
        
        this.db = new Dexie(opts.name, {})
        if(this._debug){
            console.log('name', this.db.name)
        }
        this.db.version(opts.version).stores(opts.schema)

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
    
                if(dataTab){
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
                            let stamp
                            let edit
                            try {
                                stamp = datas.stamp ? await dataTab.where('stamp').above(datas.stamp).toArray() : await dataTab.where('stamp').toArray()
                            } catch {
                                stamp = []
                            }
                            while(stamp.length){
                                datas.session = 'response'
                                datas.edit = null
                                datas.stamp = stamp.splice(stamp.length - 50, 50)
                                this.client.onSend(JSON.stringify(datas), nick)
                            }
                            try {
                                edit = datas.edit ? await dataTab.where('edit').above(datas.edit).toArray() : await dataTab.where('edit').toArray()
                            } catch {
                                edit = []
                            }
                            while(edit.length){
                                datas.session = 'response'
                                datas.stamp = null
                                datas.edit = edit.splice(edit.length - 50, 50)
                                this.client.onSend(JSON.stringify(datas), nick)
                            }
                        } else if(datas.session === 'response'){
                            if(datas.stamp){
                                let hasStamp
                                try {
                                    hasStamp = await dataTab.where('stamp').notEqual(0).last()
                                } catch {
                                    hasStamp = {}
                                }
                                const stamps = hasStamp?.stamp ? datas.stamp.filter((e) => {return e.stamp > hasStamp.stamp && e.user !== this._user}) : datas.stamp
                                try {
                                    await dataTab.bulkPut(stamps)
                                } catch (error) {
                                    console.error(error)
                                }
                                this.emit('bulk', stamps.map((data) => {return data.iden}))
                            }
                            if(datas.edit){
                                let hasEdit
                                try {
                                    hasEdit = await dataTab.where('edit').notEqual(0).last()
                                } catch {
                                    hasEdit = {}
                                }
                                const edits = hasEdit?.edit ? datas.edit.filter((e) => {return e.edit > hasEdit.edit && e.user !== this._user}) : datas.edit
                                try {
                                    await dataTab.bulkPut(edits)
                                } catch (error) {
                                    console.error(error)
                                }
                                this.emit('bulk', edits.map((data) => {return data.iden}))
                            }
                        } else {
                            return
                        }
                    } else {
                        return
                    }
                } else {
                    return
                }
            } catch {
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
                try {
                    useStamp = await table.where('stamp').notEqual(0).last()
                } catch {
                    useStamp = {}
                }
                try {
                    useEdit = await table.where('edit').notEqual(0).last()
                } catch {
                    useEdit = {}
                }
                this.client.onSend(JSON.stringify({name: table.name, stamp: useStamp?.stamp, edit: useEdit?.edit, session: 'request'}), chan)
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
        if(dataTab){
            try {
                return await dataTab.get(prop)
            } catch {
                return null
            }
        } else {
            return null
        }
    }

    async add(name, data){
        const dataTab = this.db.table(name)
        if(dataTab){
            data.stamp = data.stamp || Date.now()
            data.user = data.user || this._user
            data.iden = data.iden || crypto.randomUUID()
            data.edit = 0
            const test = await dataTab.add(data)
            // this.emit('add', test)
            this.client.onSend(JSON.stringify({name, data, user: data.user, stamp: data.stamp, iden: test, status: 'add'}))
            return test
        } else {
            return null
        }
    }

    async edit(name, prop, data){
        const dataTab = this.db.table(name)
        if(dataTab){
            const test = await dataTab.get(prop)
            if((test && test.user === this._user) && (!data.user || data.user === this._user)){
                data.edit = Date.now()
                const num = await dataTab.update(prop, data)
                // this.emit('edit', test.iden)
                this.client.onSend(JSON.stringify({name, data, iden: test.iden, user: test.user, edit: data.edit, num, status: 'edit'}))
                return test.iden
            } else {
                return null
            }
        } else {
            return null
        }
    }

    async sub(name, prop){
        const dataTab = this.db.table(name)
        if(dataTab){
            const test = await dataTab.get(prop)
            if(test){
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
                        return null
                    }
                }
            } else {
                return null
            }
        } else {
            return null
        }
    }

    async clear(name){
        const dataTab = this.db.table(name)
        if(dataTab){
            await dataTab.clear()
        }
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