import {Client} from 'relay-to-relay'
import {Dexie} from 'dexie'
import {EventEmitter} from 'events'
import "dexie-export-import"

export default class Base extends EventEmitter {
    constructor(opts){
        super()
        this._debug = opts.debug

        this._load = typeof(opts.load) === 'object' && !Array.isArray(opts.load) ? opts.load : {from: Date.now() - 86400000}

        this._span = localStorage.getItem('save') ? Number(localStorage.getItem('save')) : null

        this._sync = Boolean(opts.sync)

        this._force = opts.force === false ? opts.force : true
    
        this._keep = opts.keep === true ? opts.keep : false

        this._timer = typeof(opts.timer) === 'object' && !Array.isArray(opts.timer) ? opts.timer : {}
        this._timer.redo = this._timer.redo || 180000
        this._timer.expire = this._timer.expire || 300000
        this._timer.save = this._timer.save || 60000
    
        this._user = localStorage.getItem('user') || (() => {const test = crypto.randomUUID();localStorage.setItem('user', test);return test;})()
        
        this.client = new Client(opts.url, opts.hash, opts.rtor)
    
        opts.own = typeof(opts.own) === 'object' && !Array.isArray(opts.own) ? opts.own : {}
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
        this.db.version(opts.version).stores({...opts.own, ...opts.schema})

        this._routine = setInterval(() => {
            this._adds.clear()
            for(const [prop, update] of this._edits.entries()){
                if((Date.now() - update) > this._timer.expire){
                    this._edits.delete(prop)
                }
            }
            this._subs.clear()
        }, this._timer.redo)
        this._save = setInterval(() => {
            localStorage.setItem('save', Date.now())
        }, this._timer.save)

        this._adds = new Set()
        this._edits = new Map()
        this._subs = new Set()
        this._piecing = new Map()

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
                    if(datas.session === 'stamp'){
                        let stamps
                        if(datas.between){
                            if(!datas.includes){
                                datas.includes = {from: true, to: true}
                            }
                            stamps = await dataTab.where('stamp').between(datas.between.from, datas.between.to, datas.includes.from, datas.includes.to).toArray()
                        } else if(datas.from){
                            stamps = await dataTab.where('stamp').above(datas.from).toArray()
                        } else if(datas.to){
                            stamps = await dataTab.where('stamp').below(datas.to).toArray()
                        } else {
                            stamps = await dataTab.where('stamp').notEqual(0).toArray()
                        }
                        const count = datas.count || 25
                        while(stamps.length){
                            datas.session = 'stamps'
                            datas.stamps = stamps.splice(stamps.length - count, count)
                            datas.edits = null
                            const test = JSON.stringify(datas)
                            if(test.length < 16000){
                                this.client.onSend(test, nick)
                            } else {
                                const useID = crypto.randomUUID()
                                const pieces = Math.ceil(test.length / 15000)
                                let used = 0
                                for(let i = 1;i < (pieces + 1);i++){
                                    const slicing = i * 15000
                                    this.client.onSend(JSON.stringify({name: datas.name, piecing: 'stamps', pieces, piece: i, iden: useID, stamps: test.slice(used, slicing)}), nick)
                                    used = slicing
                                }
                            }
                        }
                    } else if(datas.session === 'stamps'){
                        if(!datas.stamps.length){
                            return
                        }
                        const arr = []
                        for(const data of datas.stamps){
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
                    } else if(datas.session === 'edit'){
                        let edits
                        if(datas.between){
                            if(!datas.includes){
                                datas.includes = {from: true, to: true}
                            }
                            edits = await dataTab.where('edit').between(datas.between.from, datas.between.to, datas.includes.from, datas.includes.to).toArray()
                        } else if(datas.from){
                            edits = await dataTab.where('edit').above(datas.from).toArray()
                        } else if(datas.to){
                            edits = await dataTab.where('edit').below(datas.to).toArray()
                        } else {
                            edits = await dataTab.where('edit').notEqual(0).toArray()
                        }
                        const count = datas.count || 25
                        while(edits.length){
                            datas.session = 'edits'
                            datas.stamps = null
                            datas.edits = edits.splice(edits.length - count, count)
                            const test = JSON.stringify(datas)
                            if(test.length < 16000){
                                this.client.onSend(test, nick)
                            } else {
                                const useID = crypto.randomUUID()
                                const pieces = Math.ceil(test.length / 15000)
                                let used = 0
                                for(let i = 1;i < (pieces + 1);i++){
                                    const slicing = i * 15000
                                    this.client.onSend(JSON.stringify({name: datas.name, piecing: 'edits', pieces, piece: i, iden: useID, edits: test.slice(used, slicing)}), nick)
                                    used = slicing
                                }
                            }
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
                } else if(datas.piecing){
                    if(datas.piecing === 'add'){
                        if(datas.user === this._user){
                            return
                        }
                        if(this._adds.has(datas.iden)){
                            return
                        }
                        if(this._piecing.has(datas.iden)){
                            const obj = this._piecing.get(datas.iden)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                                this.client.onMesh(data, nick)
                                if(obj.arr.every(Boolean)){
                                    const useData = JSON.parse(obj.arr.join(''))
                                    await dataTab.add(useData.data)
                                    this.emit('add', datas.iden)
                                    this._adds.add(datas.iden)
                                    this._piecing.delete(datas.iden)
                                }
                            }
                        } else {
                            const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                            this._piecing.set(datas.iden, obj)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                                this.client.onMesh(data, nick)
                            }
                        }
                    } else if(datas.piecing === 'edit'){
                        if(datas.user === this._user){
                            return
                        }
                        if(this._edits.has(datas.iden)){
                            const test = this._edits.get(datas.iden)
                            if(datas.edit > test){
                                if(this._piecing.has(datas.iden)){
                                    const obj = this._piecing.get(datas.iden)
                                    if(!obj.arr[datas.piece - 1]){
                                        obj.arr[datas.piece - 1] = datas.data
                                        obj.stamp = Date.now()
                                        this.client.onMesh(data, nick)
                                        if(obj.arr.every(Boolean)){
                                            const useData = JSON.parse(obj.arr.join(''))
                                            if(await dataTab.update(datas.iden, useData.data)){
                                                this.emit('edit', datas.iden)
                                            }
                                            this._edits.set(datas.iden, datas.edit)
                                            this._piecing.delete(datas.iden)
                                        }
                                    }
                                } else {
                                    const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                                    this._piecing.set(datas.iden, obj)
                                    if(!obj.arr[datas.piece - 1]){
                                        obj.arr[datas.piece - 1] = datas.data
                                        obj.stamp = Date.now()
                                        this.client.onMesh(data, nick)
                                    }
                                }
                            }
                            // else {
                            //     return
                            // }
                        } else {
                            if(this._piecing.has(datas.iden)){
                                const obj = this._piecing.get(datas.iden)
                                if(!obj.arr[datas.piece - 1]){
                                    obj.arr[datas.piece - 1] = datas.data
                                    obj.stamp = Date.now()
                                    this.client.onMesh(data, nick)
                                    if(obj.arr.every(Boolean)){
                                        const useData = JSON.parse(obj.arr.join(''))
                                        if(await dataTab.update(datas.iden, useData.data)){
                                            this.emit('edit', datas.iden)
                                        }
                                        this._edits.set(datas.iden, datas.edit)
                                        this._piecing.delete(datas.iden)
                                    }
                                }
                            } else {
                                const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                                this._piecing.set(datas.iden, obj)
                                if(!obj.arr[datas.piece - 1]){
                                    obj.arr[datas.piece - 1] = datas.data
                                    obj.stamp = Date.now()
                                    this.client.onMesh(data, nick)
                                }
                            }
                        }
                    } else if(datas.piecing === 'stamps'){
                        if(this._piecing.has(datas.iden)){
                            const obj = this._piecing.get(datas.iden)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                                if(obj.arr.every(Boolean)){
                                    const useData = JSON.parse(obj.arr.join(''))
                                    if(!useData.stamps.length){
                                        return
                                    }
                                    const arr = []
                                    for(const data of useData.stamps){
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
                                    this._piecing.delete(datas.iden)
                                }
                            }
                        } else {
                            const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                            this._piecing.set(datas.iden, obj)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                            }
                        }
                    } else if(datas.piecing === 'edits'){
                        if(this._piecing.has(datas.iden)){
                            const obj = this._piecing.get(datas.iden)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                                if(obj.arr.every(Boolean)){
                                    const useData = JSON.parse(obj.arr.join(''))
                                    if(!useData.edits.length){
                                        return
                                    }
                                    const arr = []
                                    for(const data of useData.edits){
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
                                    this._piecing.delete(datas.iden)
                                }
                            }
                        } else {
                            const obj = {stamp: Date.now(), arr: new Array(datas.pieces).fill(null)}
                            this._piecing.set(datas.iden, obj)
                            if(!obj.arr[datas.piece - 1]){
                                obj.arr[datas.piece - 1] = datas.data
                                obj.stamp = Date.now()
                            }
                        }
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
        this._connect = async (chan) => {
            console.log('connected: ' + chan)

            for(const table of this.db.tables){
                if(this._sync === true){
                    this.client.onSend(JSON.stringify({name: table.name, session: 'stamp'}), chan)
                    this.client.onSend(JSON.stringify({name: table.name, session: 'edit'}), chan)
                } else if(this._sync === null){
                    if(this._span){
                        this.client.onSend(JSON.stringify({between: {from: this._span, to: Date.now()}, name: table.name, session: 'stamp'}), chan)
                        this.client.onSend(JSON.stringify({between: {from: this._span, to: Date.now()}, name: table.name, session: 'edit'}), chan)
                    } else {
                        this.client.onSend(JSON.stringify({...this._load, name: table.name, session: 'stamp'}), chan)
                        this.client.onSend(JSON.stringify({...this._load, name: table.name, session: 'edit'}), chan)
                    }
                } else if(this._sync === false){
                    const s = await table.where('stamp').notEqual(0).last()
                    const e = await table.where('edit').notEqual(0).last()
                    if(s){
                        this.client.onSend(JSON.stringify({from: s.stamp - 43200000, name: table.name, session: 'stamp'}), chan)
                    }
                    if(e){
                        this.client.onSend(JSON.stringify({from: e.edit - 43200000, name: table.name, session: 'edit'}), chan)
                    }
                } else {
                    continue
                }
            }
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
        const useData = JSON.stringify({name, data, user: data.user, stamp: data.stamp, iden: test, status: 'add'})
        if(useData.length < 16000){
            this.client.onSend(useData)
        } else {
            const pieces = Math.ceil(useData.length / 15000)
            let used = 0
            for(let i = 1;i < (pieces + 1);i++){
                const slicing = i * 15000
                this.client.onSend(JSON.stringify({name, data: useData.slice(used, slicing), user: data.user, stamp: data.stamp, iden: test, piecing: 'add', pieces, pice: i}), nick)
                used = slicing
            }
        }
        return test
    }

    async edit(name, prop, data){
        const dataTab = this.db.table(name)
        const test = await dataTab.get(prop)
        if((test && test.user === this._user) && (!data.user || data.user === this._user)){
            data.edit = Date.now()
            const num = await dataTab.update(prop, data)
            // this.emit('edit', test.iden)
            const useData = JSON.stringify({name, data, iden: test.iden, user: test.user, edit: data.edit, num, status: 'edit'})
            if(useData.length < 16000){
                this.client.onSend(useData)
            } else {
                const pieces = Math.ceil(useData.length / 15000)
                let used = 0
                for(let i = 1;i < (pieces + 1);i++){
                    const slicing = i * 15000
                    this.client.onSend(JSON.stringify({name, data: useData.slice(used, slicing), iden: test.iden, user: test.user, edit: data.edit, num, piecing: 'edit', pieces, piece: i}), nick)
                    used = slicing
                }
            }
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

    load(name, session, data = {}){
        const dataTab = this.db.table(name)
        data.name = dataTab.name
        data.session = session
        this.client.onSend(JSON.stringify(data))
    }

    async getDB(blob, opts){
        return await this.db.import(blob, opts)
    }

    async postDB(opts){
        return await this.db.export(opts)
    }

    quit(){
        clearInterval(this._routine)
        clearInterval(this._save)
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