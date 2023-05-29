import { Transform } from "node:stream";
import { inflateSync } from "node:zlib";
import { createReadStream } from "node:fs";
import Pbf from "pbf";
import { Blob as BlobData, BlobHeader } from "./proto/fileformat";
import { HeaderBlock, PrimitiveBlock } from "./proto/osmformat";
import { PathLike } from "fs";

const debug = false; // print some stats

const memberTypes = ["node", "way", "relation"];

function assert(cond: unknown, message?: string) {
  if (!cond) throw new Error(message || "input format error");
}

/** Checks that given arrays are parallel */
function assertArrays(...args: any[]) {
  let length = -1;
  for (const arg of args) {
    assert(Array.isArray(arg));
    if (length < 0) length = arg.length;
    else assert(length == arg.length);
  }
}

/** Checks that given obj has properties */
function isEmpty(obj: {
  version?: number | undefined;
  timestamp?: number | undefined;
  changeset?: number | undefined;
  uid?: number | undefined;
  user?: string | undefined;
  visible?: boolean | undefined;
}) {
  for (let p in obj) return false;
  return true;
}

export class OSMTransform extends Transform {
  with: { withTags: {
    node: string[];
    way: any[];
    relation: string[];
}; withInfo: boolean; writeRaw: any };
  buffer?: Buffer;
  offset: number;
  status: number;
  needed?: number;
  inflate_ns?: bigint;

  constructor(
    osmopts: { withTags?: boolean | any; withInfo?: boolean; writeRaw?: boolean } = { withTags: true, withInfo: false, writeRaw: false },
    opts: { withTags?: boolean | any; withInfo?: boolean; writeRaw?: boolean } = { withTags: true, withInfo: false, writeRaw: false}
  ) {
    super(
      Object.assign({}, opts, {
        writableObjectMode: false,
        writableHighWaterMark: 0,
        readableObjectMode: true,
        readableHighWaterMark: 1,
      })
    );
    this.with = {
      withTags: with_tags(osmopts.withTags ?? true),
      withInfo: osmopts.withInfo ?? false,
      writeRaw: osmopts.writeRaw ?? false,
    };
    /** @type {Buffer} */
    this.buffer = undefined;
    this.offset = 0; // current offset in the buffer
    this.status = 0; // 0: header length, 1: header,
    // 2: OSMHeader, 3: OSMData
    this.needed = 4; // number of bytes required in the buffer
    if (debug) {
      this.inflate_ns = 0n; // inflateSync total time
    }
  }

  _transform(chunk: Buffer, encoding: any, next: () => any) {
    if (this.buffer === undefined) this.buffer = chunk ?? undefined;
    else {
      this.buffer = Buffer.concat([this.buffer.subarray(this.offset), chunk]);
      this.offset = 0;
    }
    const pbf = new Pbf(this.buffer);
    if (this.status == 0) pbf.length = 0;
    else pbf.length = this.needed!;

    // eslint-disable-next-line no-constant-condition
    while (true) {
      if (
        this.buffer &&
        this.needed &&
        this.buffer.length - this.offset < this.needed
      )
        return next(); // _transform will be called with the next chunk

      if (this.status == 0) {
        // expecting int32 with the header length
        advance(4);
        const l = this.buffer?.readUInt32BE(this.offset);
        advance(l);
        this.offset += this.needed ?? 0;
        this.needed = l; // header of this length follows
        this.status = 1;
      } else if (this.status == 1) {
        // expecting BlobHeader
        const header = BlobHeader.read(pbf);
        assert(
          header.type == "OSMHeader" || header.type == "OSMData",
          "input sequence error"
        );
        this.offset += this.needed ?? 0;
        this.needed = header.datasize; // data of this length follows
        advance(header.datasize);
        this.status = header.type == "OSMHeader" ? 2 : 3;
      } else if (this.status == 2) {
        // expecting OSMHeader
        const blob = BlobData.read(pbf);
        const buf = blob.zlib_data ? inflateSync(blob.zlib_data) : blob.raw;
        assert(buf, `inflating ${blob.data} not implemented`);
        const header = HeaderBlock.read(new Pbf(buf));
        this.push([header]);
        this.offset += this.needed ?? 0;
        this.needed = 4; // next header length follows
        this.status = 0;
      } else if (this.status == 3) {
        // expecting OSMData
        const blob = BlobData.read(pbf);
        assert(blob.zlib_data, `inflating ${blob.data} not implemented`);
        if (this.with.writeRaw) this.push(blob.zlib_data);
        else {
          const start = process.hrtime.bigint();
          const buf = inflateSync(blob.zlib_data);
          if (debug) this.inflate_ns! += process.hrtime.bigint() - start;
          this.push(parse(buf, this));
        }
        this.offset += this.needed ?? 0;
        this.needed = 4; // next header length follows
        this.status = 0;
      }
    }

    function advance(step: number) {
      pbf.pos = pbf.length;
      pbf.length += step;
    }
  }

  _flush(callback: () => void) {
    if (debug) {
      let sec = Number(this.inflate_ns) * 1e-9;
      console.log(`inflateSync took ${sec.toFixed(3)} sec.`);
    }
    assert(this.buffer?.length == this.offset && this.status == 0);
    callback();
  }
}

function with_tags(opt: boolean | any) {
  if (typeof opt == "boolean") return { node: opt, way: opt, relation: opt };
  let result: any = {};
  if (typeof opt == "object" && opt != null) {
    for (let k of ["node", "way", "relation"]) {
      let b = opt[k] ?? true;
      if (typeof b == "boolean") result[k] = b;
      else if (Array.isArray(b)) result[k] = b.length > 0 ? new Set(b) : false;
      else {
        result = null;
        break;
      }
    }
  }
  if (result) return result;
  throw new Error(`wrong withTags option.`);
}

/**
 * Returns array of either nodes, ways or relations.
 * @param {Buffer} buf Inflated OSMData block
 * @param {OSMTransform | OSMOptions} that
 */
export function parse(buf: ArrayBuffer | undefined, that: any) {
  const data: any = PrimitiveBlock.read(new Pbf(buf));
  if (that instanceof OSMTransform) {
    data.withTags = that.with.withTags;
    data.withInfo = that.with.withInfo;
  } else {
    data.withTags = with_tags(that ?? true);
    data.withInfo = that.withInfo ?? false;
  }
  data.strings = data.stringtable.s.map(
    (b: any) => b.toString("utf8")
  );
  data.date_granularity = data.date_granularity || 1000;
  data.granularity =
    !data.granularity || data.granularity == 100 ? 1e7 : 1e9 / data.granularity;
  data.lat_offset *= 1e-9;
  data.lon_offset *= 1e-9;
  const batch: any[] = [];
  for (const p of data.primitivegroup) {
    if (p.changesets && p.changesets.length > 0)
      throw new Error("changesets not implemented");
    if (p.nodes) {
      for (const n of p.nodes) batch.push(parse_node(n, data));
    }
    if (p.dense) batch.push(...parse_dense(p.dense, data));
    if (p.ways) {
      for (const w of p.ways) batch.push(parse_way(w, data));
    }
    if (p.relations) {
      for (const r of p.relations) batch.push(parse_rel(r, data));
    }
  }
  return batch;
}

function parse_rel(
  r: {
    memids: string | any[];
    types: (string | number)[];
    roles_sid: (string | number)[];
    id: any;
    keys: string | any[];
    vals: (string | number)[];
    info: any;
  },
  data: { strings: any; withTags: { relation: boolean }; withInfo: any; date_granularity: number }
) {
  const strings = data.strings;
  assertArrays(r.memids, r.types, r.roles_sid);
  const members = Array(r.memids.length);
  let ref = 0;
  for (let i = 0; i < r.memids.length; i++) {
    members[i] = {
      type: memberTypes[r.types[i]],
      ref: (ref += r.memids[i]),
      role: strings[r.roles_sid[i]],
    };
  }
  const rel: {
    type: string;
    id: number;
    members: any[];
    tags?: any;
    info?: any;
  } = {
    type: "relation",
    id: r.id,
    members: members,
    tags: undefined,
    info: undefined,
  };
  if (data.withTags.relation) {
    assertArrays(r.keys, r.vals);
    const filter: boolean | any =
      data.withTags.relation === true ? false : data.withTags.relation;
    const tags = {};
    for (let i = 0; i < r.keys.length; i++) {
      const key = strings[r.keys[i]],
        val = strings[r.vals[i]];
      if (!filter || filter.has(key)) tags[key] = val;
    }
    if (!isEmpty(tags)) rel.tags = tags;
  }
  if (data.withInfo && r.info) {
    const info = fill_info(data, r.info);
    if (!isEmpty(info)) rel.info = info;
  }
  return rel;
}

function parse_way(
  w: {
    refs: string | any[];
    id: any;
    keys: string | any[];
    vals: (string | number)[];
    info: any;
  },
  data: { strings: any; withTags: { way: boolean }; withInfo: any, date_granularity: number }
) {
  const strings = data.strings;
  const refs = Array(w.refs.length);
  let ref = 0;
  for (let i = 0; i < w.refs.length; i++) {
    refs[i] = ref += w.refs[i];
  }
  const way: { type: string; id: number; refs: any[]; tags?: any; info?: any } =
    {
      type: "way",
      id: w.id,
      refs: refs,
      tags: undefined,
      info: undefined,
    };
  if (data.withTags.way) {
    assertArrays(w.keys, w.vals);
    const filter: boolean | any  = data.withTags.way === true ? false : data.withTags.way;
    const tags = {};
    for (let i = 0; i < w.keys.length; i++) {
      const key = strings[w.keys[i]],
        val = strings[w.vals[i]];
      if (!filter || filter.has(key)) tags[key] = val;
    }
    if (!isEmpty(tags)) way.tags = tags;
  }
  if (data.withInfo && w.info) {
    const info = fill_info(data, w.info);
    if (!isEmpty(info)) way.info = info;
  }
  return way;
}

function parse_node(n: any, data: any) {
  const strings = data.strings;
  const node: {
    type: string;
    id: number;
    lat: number;
    lon: number;
    tags?: any;
    info?: any;
  } = {
    type: "node",
    id: n.id,
    lat: data.lat_offset + n.lat / data.granularity,
    lon: data.lon_offset + n.lon / data.granularity,
    tags: undefined,
    info: undefined,
  };
  if (data.withTags.node) {
    assertArrays(n.keys, n.vals);
    const filter = data.withTags.node === true ? false : data.withTags.node;
    const tags = {};
    for (let i = 0; i < n.keys.length; i++) {
      const key = strings[n.keys[i]],
        val = strings[n.vals[i]];
      if (!filter || filter.has(key)) tags[key] = val;
    }
    if (!isEmpty(tags)) node.tags = tags;
  }
  if (data.withInfo && n.info) {
    const info = fill_info(data, n.info);
    if (!isEmpty(info)) node.info = info;
  }
  return node;
}

function parse_dense(
  dense: {
    denseinfo: any;
    id: string | any[];
    lat: number[];
    lon: number[];
    keys_vals: string | any[];
  },
  data: {
    strings: any;
    lat_offset: number;
    granularity: number;
    lon_offset: number;
    withTags: { node: boolean };
    withInfo: any;
    date_granularity: number;
  }
) {
  const dinfo = dense.denseinfo;
  let id = 0,
    lat = 0,
    lon = 0;
  let timestamp = 0,
    changeset = 0;
  let uid = 0,
    user_sid = 0;
  //
  assertArrays(dense.id, dense.lat, dense.lon);
  const strings = data.strings;
  const nodes = Array(dense.id.length);
  let j = 0;
  for (let i = 0; i < dense.id.length; i++) {
    id += dense.id[i];
    lat += dense.lat[i];
    lon += dense.lon[i];
    const node: {
      type: string;
      id: number;
      lat: number;
      lon: number;
      tags?: any;
      info?: any;
    } = {
      type: "node",
      id: id,
      lat: data.lat_offset + lat / data.granularity,
      lon: data.lon_offset + lon / data.granularity,
      tags: undefined,
      info: undefined,
    };
    if (data.withTags.node && dense.keys_vals.length > 0) {
      const filter: boolean | any  = data.withTags.node === true ? false : data.withTags.node;
      const tags = {};
      while (dense.keys_vals[j] !== 0) {
        const key = strings[dense.keys_vals[j]];
        const val = strings[dense.keys_vals[j + 1]];
        if (!filter || filter.has(key)) tags[key] = val;
        j += 2;
      }
      j++;
      if (!isEmpty(tags)) node.tags = tags;
    }
    if (data.withInfo && dinfo) {
      assertArrays(
        dense.id,
        dinfo.timestamp,
        dinfo.changeset,
        dinfo.uid,
        dinfo.user_sid,
        dinfo.version
      );
      timestamp += dinfo.timestamp[i];
      changeset += dinfo.changeset[i];
      uid += dinfo.uid[i];
      user_sid += dinfo.user_sid[i];
      //
      const info = fill_info(data, {
        version: dinfo.version[i],
        timestamp: timestamp,
        changeset: changeset,
        uid: uid,
        user_sid: user_sid,
        visible: dinfo.visible[i],
      });
      if (!isEmpty(info)) node.info = info;
    }
    nodes[i] = node;
  }
  return nodes;
}

function fill_info(
  data: { date_granularity: number; strings: { [x: string]: any } },
  info: {
    version: any;
    timestamp: any;
    changeset: any;
    uid: any;
    user_sid: any;
    visible: any;
  }
) {
  const ret: {
    version?: number;
    timestamp?: number;
    changeset?: number;
    uid?: number;
    user?: string;
    visible?: boolean;
  } = {};
  if (info.version !== 0) ret.version = info.version;
  if (info.timestamp !== 0)
    ret.timestamp = info.timestamp * data.date_granularity;
  if (info.changeset !== 0) ret.changeset = info.changeset;
  if (info.uid !== 0) ret.uid = info.uid;
  if (info.user_sid !== 0) {
    let s = data.strings[info.user_sid];
    if (s) ret.user = s;
  }
  if (info.visible === false) ret.visible = false;
  return ret;
}

export async function* createOSMStream(
  file: PathLike,
  opts?: { withTags?: boolean; withInfo?: boolean; writeRaw?: boolean } | {
    withInfo: boolean;
    withTags: {
        node: string[];
        way: any[];
        relation: string[];
    };
}
) {
  const readable = createReadStream(file).pipe(new OSMTransform(opts));
  for await (const chunk of readable) {
    for (const item of chunk) yield item;
  }
}
