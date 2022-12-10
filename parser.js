import { Transform } from 'node:stream';
import { inflateSync } from 'node:zlib';
import { createReadStream } from 'node:fs';
import Pbf from 'pbf';
import { Blob as BlobData, BlobHeader } from './fileformat.js';
import { HeaderBlock, PrimitiveBlock } from './osmformat.js';

const memberTypes = ['node', 'way', 'relation'];

function assert(cond, message) {
    if (!cond)
        throw new Error(message || 'input format error');
}

/** Checks that given arrays are parallel */
function assertArrays(...args) {
    let length = -1;
    for (const arg of args) {
        assert(Array.isArray(arg));
        if (length < 0)
            length = arg.length;
        else
            assert(length == arg.length);
    }
}

/** Checks that given obj has properties */
function isEmpty(obj) {
    for (let p in obj)
        return false;
    return true;
}

export class OSMTransform extends Transform {
    constructor(osmopts = { withTags: true, withInfo: false }, opts = {}) {
        super(Object.assign({}, opts, {
            writableObjectMode: false,
            writableHighWaterMark: 0,
            readableObjectMode: true,
            readableHighWaterMark: 1
        }));
        this.with = {
            withTags: osmopts.withTags ?? true,
            withInfo: osmopts.withInfo ?? false,
            batchMode: osmopts.batchMode ?? false
        };
        /** @type {Buffer} */
        this.buffer = null;
        this.offset = 0;     // current offset in the buffer
        this.status = 0;     // 0: header length, 1: header, 2: data
        this.needed = 4;     // number of bytes required in the buffer
    }

    _transform(chunk, encoding, next) {
        if (this.buffer == null)
            this.buffer = chunk;
        else {
            this.buffer = Buffer.concat([this.buffer.subarray(this.offset), chunk]);
            this.offset = 0;
        }
        const pbf = new Pbf(this.buffer);
        if (this.status == 0)
            pbf.length = 0;
        else
            pbf.length = this.needed;

        // eslint-disable-next-line no-constant-condition
        while (true) {
            if (this.buffer.length - this.offset < this.needed)
                return next();       // _transform will be called with the next chunk

            if (this.status == 0) {  // expecting int32 with the header length
                fwd(4);
                const l = this.buffer.readUInt32BE(this.offset);
                fwd(l);
                this.offset += this.needed;
                this.needed = l;     // header of this length follows
                this.status = 1;
            }
            else if (this.status == 1) {   // expecting BlobHeader
                const header = BlobHeader.read(pbf);
                assert(header.type == 'OSMHeader' || header.type == 'OSMData',
                    'input sequence error');
                this.offset += this.needed;
                this.needed = header.datasize;   // data of this length follows
                fwd(header.datasize);
                this.status = header.type == 'OSMHeader' ? 2 : 3
            }
            else if (this.status == 2) {   // expectong OSMHeader
                const blob = BlobData.read(pbf);
                const buf = blob.zlib_data ? inflateSync(blob.zlib_data) : blob.raw;
                assert(buf, `inflating ${blob.data} not implemented`);
                const header = HeaderBlock.read(new Pbf(buf));
                this.push(this.with.batchMode ? [header] : header);
                this.offset += this.needed;
                this.needed = 4;   // next header length follows
                this.status = 0;
            }
            else if (this.status == 3) {    // expecting OSMData
                const blob = BlobData.read(pbf);
                const buf = blob.zlib_data ? inflateSync(blob.zlib_data) : blob.raw;
                assert(buf, `inflating ${blob.data} not implemented`);
                const data = PrimitiveBlock.read(new Pbf(buf));
                data.strings = data.stringtable.s.map(b => b.toString('utf8'));
                data.date_granularity = data.date_granularity || 1000;
                data.granularity = (!data.granularity || data.granularity == 100) ? 1e7
                    : 1e9 / data.granularity;
                data.lat_offset *= 1e-9;
                data.lon_offset *= 1e-9;
                data.withTags = this.with.withTags;
                data.withInfo = this.with.withInfo;
                const batch = [];
                for (const p of data.primitivegroup) {
                    if (p.changesets && p.changesets.length > 0)
                        throw new Error('changesets not implemented');
                    if (p.nodes) {
                        for (const n of p.nodes)
                            batch.push(parse_node(n, data));
                    }
                    if (p.dense)
                        batch.push(...parse_dense(p.dense, data));
                    if (p.ways) {
                        for (const w of p.ways)
                            batch.push(parse_way(w, data));
                    }
                    if (p.relations) {
                        for (const r of p.relations)
                            batch.push(parse_rel(r, data));
                    }
                }
                this.push(batch);
                this.offset += this.needed;
                this.needed = 4;   // next header length follows
                this.status = 0;
            }
        }

        function fwd(step) {
            pbf.pos = pbf.length;
            pbf.length += step;
        }
    }
}

function parse_rel(r, data) {
    const strings = data.strings;
    assertArrays(r.memids, r.types, r.roles_sid);
    const members = [];
    let ref = 0;
    for (let i = 0; i < r.memids.length; i++) {
        members.push({
            type: memberTypes[r.types[i]],
            ref: ref += r.memids[i],
            role: strings[r.roles_sid[i]]
        });
    }
    assert(members.length > 0, 'no members in relation ' + r.id);
    const rel = {
        type: 'relation',
        id: r.id,
        members: members
    }
    if (data.withTags) {
        assertArrays(r.keys, r.vals);
        const tags = {};
        for (let i = 0; i < r.keys.length; i++) {
            const key = r.keys[i], val = r.vals[i];
            tags[strings[key]] = strings[val];
        }
        if (!isEmpty(tags))
            rel.tags = tags;
    }
    if (data.withInfo && r.info) {
        const info = fill_info(data, r.info);
        if (!isEmpty(info))
            rel.info = info;
    }
    return rel;
}

function parse_way(w, data) {
    const strings = data.strings;
    const refs = [];
    let ref = 0;
    for (let i = 0; i < w.refs.length; i++) {
        refs.push(ref += w.refs[i]);
    }
    assert(refs.length > 0, 'no nodes in way ' + w.id);
    const way = {
        type: 'way',
        id: w.id,
        refs: refs
    }
    if (data.withTags) {
        assertArrays(w.keys, w.vals);
        const tags = {};
        for (let i = 0; i < w.keys.length; i++) {
            tags[strings[w.keys[i]]] = strings[w.vals[i]];
        }
        if (!isEmpty(tags))
            way.tags = tags;
    }
    if (data.withInfo && w.info) {
        const info = fill_info(data, w.info);
        if (!isEmpty(info))
            way.info = info;
    }
    return way;
}

function parse_node(n, data) {
    const strings = data.strings;
    const node = {
        type: 'node',
        id: n.id,
        lat: data.lat_offset + n.lat / data.granularity,
        lon: data.lon_offset + n.lon / data.granularity
    }
    if (data.withTags) {
        assertArrays(n.keys, n.vals);
        const tags = {};
        for (let i = 0; i < n.keys.length; i++) {
            tags[strings[n.keys[i]]] = strings[n.vals[i]]
        }
        if (!isEmpty(tags))
            node.tags = tags;
    }
    if (data.withInfo && n.info) {
        const info = fill_info(data, n.info);
        if (!isEmpty(info))
            node.info = info;
    }
    return node;
}

function parse_dense(dense, data) {
    const dinfo = dense.denseinfo;
    let id = 0, lat = 0, lon = 0;
    let timestamp = 0, changeset = 0;
    let uid = 0, user = 0;
    //
    assertArrays(dense.id, dense.lat, dense.lon);
    const strings = data.strings;
    const nodes = [];
    let j = 0;
    for (let i = 0; i < dense.id.length; i++) {
        id += dense.id[i];
        lat += dense.lat[i];
        lon += dense.lon[i];
        const node = {
            type: 'node',
            id: id,
            lat: data.lat_offset + lat / data.granularity,
            lon: data.lon_offset + lon / data.granularity
        }
        if (data.withTags && dense.keys_vals.length > 0) {
            const tags = {};
            while (dense.keys_vals[j] !== 0) {
                tags[strings[dense.keys_vals[j]]] = strings[dense.keys_vals[j + 1]];
                j += 2;
            }
            j++;
            if (!isEmpty(tags))
                node.tags = tags;
        }
        if (data.withInfo && dinfo) {
            assertArrays(dense.id, dinfo.timestamp, dinfo.changeset, dinfo.uid,
                dinfo.user_sid, dinfo.version);
            timestamp += dinfo.timestamp[i];
            changeset += dinfo.changeset[i];
            uid += dinfo.uid[i];
            user += dinfo.user_sid[i];
            //
            const info = {};
            info.version = dinfo.version[i];
            let num = timestamp * data.date_granularity;
            info.timestamp = new Date(num).toISOString().substring(0, 19) + 'Z';
            info.changeset = changeset;
            if (uid)
                info.uid = uid;
            if (strings[user])
                info.user = strings[user]
            if (dinfo.visible[i] === false)
                info.visible = false;
            if (!isEmpty(info))
                node.info = info;
        }
        nodes.push(node);
    }
    return nodes;
}

function fill_info(data, info) {
    const ret = {};
    if (info.version !== 0)
        ret.version = info.version;
    if (info.timestamp !== 0) {
        let num = info.timestamp * data.date_granularity;
        ret.timestamp = new Date(num).toISOString().substring(0, 19) + 'Z';
    }
    if (info.changeset !== 0)
        ret.changeset = info.changeset;
    if (info.uid !== 0)
        ret.uid = info.uid;
    if (info.user_sid !== 0) {
        let s = data.strings[info.user_sid];
        if (s)
            ret.user = s;
    }
    if (info.visible === false)
        ret.visible = false;
    return ret;
}

export async function* createOSMStream(file, opts) {
    const readable = createReadStream(file)
        .pipe(new OSMTransform(opts));
    for await (const chunk of readable) {
        if (Array.isArray(chunk)) {
            for (const item of chunk)
                yield item;
        } else
            yield chunk;
    }
}