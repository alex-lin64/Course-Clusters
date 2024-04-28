function traverseNatives(object, seen, idToNative, nativeToId) {
  if (object === undefined || object === null || seen.has(object)) {
    return;
  }
  seen.add(object);
  if (typeof object === "function") {
    const id = seen.size.toString();
    idToNative.set(id, object);
    nativeToId.set(object, id);
  }
  const props = Object.getOwnPropertyDescriptors(object);
  for (const { value } of Object.values(props)) {
    traverseNatives(value, seen, idToNative, nativeToId);
  }
}

const idToNative = new Map();
const nativeToId = new Map();
traverseNatives(globalThis, new Set(), idToNative, nativeToId);

const formats = [
  {
    matches: (object) => object === undefined,
    kind: "undefined",
    ser: (object) => "undefined",
    de: (value, evil) => undefined,
  },
  {
    matches: (o) => {
      return o === null || ["number", "string", "boolean"].includes(typeof o);
    },
    kind: "leaf",
    ser: (object) => object,
    de: (value, evil) => value,
  },
  {
    matches: (object) => typeof object === "function",
    kind: "function",
    ser: (object) => {
      if (nativeToId.has(object)) {
        return { scope: "native", value: nativeToId.get(object) };
      }
      return { scope: "defined", value: object.toString() };
    },
    de: ({ scope, value }, evil) => {
      if (scope === "native") {
        return idToNative.get(value);
      }
      if (scope === "defined") {
        value = value.replace(/^\w+\(/, "function(");
        value = `(${value})`;
        return evil(value);
      }
    },
  },
  {
    matches: (object) => object instanceof Date,
    kind: "date",
    ser: (object) => object.toISOString(),
    de: (value, evil) => new Date(value),
  },
  {
    matches: (object) =>
      object instanceof Error || (object.name || "").includes("Error"),
    kind: "error",
    ser: (object) => objectifyError(object),
    de: ({ message, cause, stack }, evil) => {
      const ret = new Error(message, { cause });
      ret.stack = stack;
      return ret;
    },
  },
];


function objectifyError(error) {
    if (!error) {
        return error;
    }
    return {
        message: error.message,
        cause: objectifyError(error.cause),
        stack: error.stack,
    };
}

function mapObject(object, func) {
  const entries = Object.entries(object).map(([k, v]) => [k, func(v)]);
  return Object.fromEntries(entries);
}

function serialize(object) {
  let idState = 0;
  const objectToReference = new Map();
  const idToObject = new Map();
  const encode = (object) => {
    for (const { matches, kind, ser } of formats) {
      if (matches(object)) {
        return { kind, value: ser(object) };
      }
    }
    if (objectToReference.has(object)) {
      return objectToReference.get(object);
    }
    const id = idState++;
    const kind = object instanceof Array ? "array" : "object";
    const reference = { kind: "reference", value: id };
    objectToReference.set(object, reference);
    const represented =
      kind === "array"
        ? { kind, value: object.map(encode) }
        : { kind, value: mapObject(object, encode) };
    idToObject.set(id.toString(), represented);
    return reference;
  };
  const root = encode(object);
  return JSON.stringify({
    idToObject: Object.fromEntries(idToObject),
    root,
  });
}

function deserialize(string, evil) {
  evil = evil || eval;
  const { idToObject, root } = JSON.parse(string);
  const cannonical = new Map();
  const decode = ({ kind, value }) => {
    for (const { kind: k, de } of formats) {
      if (k === kind) {
        return de(value, evil);
      }
    }
    if (kind === "reference") {
      if (!cannonical.has(value)) {
        const referenceKind = idToObject[value].kind;
        const newObject = referenceKind === "array" ? [] : {};
        // add the object to the map before we call decode
        cannonical.set(value, newObject);
        const decoding = decode(idToObject[value]);
        return Object.assign(newObject, decoding);
      }
      return cannonical.get(value);
    }
    if (kind === "array") {
      return value.map(decode);
    }
    if (kind === "object") {
      return mapObject(value, decode);
    }
  };
  return decode(root);
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};
