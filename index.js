import {Client} from 'relay-to-relay'
import {Dexie} from 'dexie'
import {EventEmitter} from 'events'

export default class Base extends EventEmitter {
    constructor(opts){
        super()
        this.debug = opts.debug

        this.force = opts.force === false ? opts.force : true
    
        this.keep = opts.keep === true ? opts.keep : false
    
        this.user = localStorage.getItem('user') || (() => {const test = crypto.randomUUID();localStorage.setItem('user', test);return test;})()
        
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
        if(this.debug){
            console.log('name', this.db.name)
        }
        this.db.version(opts.version).stores(opts.schema)
    
        this.adds = new Set()
        this.edits = new Map()
        this.subs = new Set()
    
        this.routine = setInterval(() => {
            this.adds.clear()
            for(const [prop, update] of this.edits.entries()){
                if((Date.now() - update) > 300000){
                    this.edits.delete(prop)
                }
            }
            this.subs.clear()
        }, 180000)

        this.client.on('connect', this.#connect)
        this.client.on('error', this.#err)
        this.client.on('disconnect', this.#disconnect)
        this.client.on('message', this.#message)
    }

    async #message(data, nick){
        try {
            if(this.debug){
                console.log('Received Message: ', typeof(data), data)
            }

            const datas = JSON.parse(data)

            const dataTab = this.db.table(datas.name)

            if(dataTab){
                if(datas.status){
                    if(datas.user === this.user){
                        return
                    }
                    if(datas.status === 'add'){
                        if(this.adds.has(datas.iden)){
                            return
                        }
                        await dataTab.add(datas.data)
                        this.emit('add', datas.iden)
                        this.adds.add(datas.iden)
                        this.client.onMesh(data, nick)
                    } else if(datas.status === 'edit'){
                        if(this.edits.has(datas.iden)){
                            const test = this.edits.get(datas.iden)
                            if(datas.edit > test){
                                await dataTab.update(datas.iden, datas.data)
                                this.emit('edit', datas.iden)
                                this.edits.set(datas.iden, datas.edit)
                                this.client.onMesh(data, nick)
                            } else {
                                return
                            }
                        } else {
                            await dataTab.update(datas.iden, datas.data)
                            this.emit('edit', datas.iden)
                            this.edits.set(datas.iden, datas.edit)
                            this.client.onMesh(data, nick)
                        }
                    } else if(datas.status === 'sub'){
                        if(this.subs.has(datas.iden)){
                            return
                        }
                        if(!this.keep){
                            await dataTab.delete(datas.iden)
                            this.emit('sub', datas.iden)
                        }
                        this.subs.add(datas.iden)
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
                            const stamps = hasStamp?.stamp ? datas.stamp.filter((e) => {return e.stamp > hasStamp.stamp && e.user !== this.user}) : datas.stamp
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
                            const edits = hasEdit?.edit ? datas.edit.filter((e) => {return e.edit > hasEdit.edit && e.user !== this.user}) : datas.edit
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
                console.log('no db or table')
            }
        } catch {
            return
        }
    }

    #disconnect(chan){
        console.log('disconnected: ' + chan)
    }

    #err(e, chan){
        console.error(e, chan)
    }

    #connect(chan){
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
            data.user = data.user || this.user
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
            if((test && test.user === this.user) && (!data.user || data.user === this.user)){
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
                if(this.force){
                    await dataTab.delete(prop)
                    // this.emit('sub', test.iden)
                    if(test.user === this.user){
                        this.client.onSend(JSON.stringify({name, iden: test.iden, user: test.user, status: 'sub'}))
                    }
                    return test.iden
                } else {
                    if(test.user === this.user){
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
        clearInterval(this.routine)
        this.edits.clear()
        this.client.off('connect', this.#connect)
        this.client.off('error', this.#err)
        this.client.off('message', this.#message)
        this.client.off('disconnect', this.#disconnect)
        this.client.end()
        this.db.close()
    }
}