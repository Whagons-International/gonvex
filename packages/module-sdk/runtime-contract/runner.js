// Shared behavioral cases. This file is also the real Rust isolate's ESM artifact.
const fromHex = (value) => Uint8Array.from(value.match(/../g) ?? [], (byte) => parseInt(byte, 16));
const hex = (value) => Array.from(new Uint8Array(value), (byte) => byte.toString(16).padStart(2, "0")).join("");
const importKey = (key, algorithm, usages) => crypto.subtle.importKey(key.format, key.jwk ?? fromHex(key.hex), algorithm, false, usages);
const cachedKeys = new Map();

export async function contract(ctx, vector) {
  switch (vector.run) {
    case "digest": return hex(await crypto.subtle.digest(vector.algorithm, fromHex(vector.data)));
    case "cached-key": {
      if (!cachedKeys.has(vector.id)) cachedKeys.set(vector.id, await importKey(vector.key, vector.algorithm, ["sign"]));
      return hex(await crypto.subtle.sign(vector.algorithm, cachedKeys.get(vector.id), fromHex(vector.data)));
    }
    case "signature": {
      const privateKey = await importKey(vector.key, vector.algorithm, ["sign", ...(vector.publicKey ? [] : ["verify"])]);
      const publicKey = vector.publicKey ? await importKey(vector.publicKey, vector.algorithm, ["verify"]) : privateKey;
      const data = fromHex(vector.data);
      const signature = await crypto.subtle.sign(vector.algorithm, privateKey, data);
      const invalid = new Uint8Array(signature.slice(0));
      invalid[0] ^= 1;
      return { signature: hex(signature), valid: await crypto.subtle.verify(vector.algorithm, publicKey, signature, data), invalid: await crypto.subtle.verify(vector.algorithm, publicKey, invalid, data), empty: await crypto.subtle.verify(vector.algorithm, publicKey, new Uint8Array(), data) };
    }
    case "aes": {
      const key = await importKey(vector.key, "AES-GCM", ["encrypt", "decrypt"]);
      const algorithm = { name: "AES-GCM", iv: fromHex(vector.iv), additionalData: fromHex(vector.aad ?? "") };
      const encrypted = await crypto.subtle.encrypt(algorithm, key, fromHex(vector.plain));
      // The stored legacy format separates the final 16-byte tag. Reassemble
      // that existing ciphertext, rather than only decrypting our own output.
      const sealed = fromHex(vector.sealed);
      const ciphertext = sealed.slice(0, -16), tag = sealed.slice(-16);
      const legacy = new Uint8Array(ciphertext.length + tag.length);
      legacy.set(ciphertext); legacy.set(tag, ciphertext.length);
      const plain = await crypto.subtle.decrypt(algorithm, key, legacy);
      legacy[legacy.length - 1] ^= 1;
      let rejected = false;
      try { await crypto.subtle.decrypt(algorithm, key, legacy); } catch (error) { rejected = error.name === "OperationError"; }
      return { sealed: hex(encrypted), plain: hex(plain), rejected };
    }
    case "crypto-errors": {
      const key = await crypto.subtle.importKey("raw", new Uint8Array(32), "AES-GCM", false, ["encrypt"]);
      const errors = [];
      for (const run of [
        () => crypto.subtle.sign("HMAC", key, new Uint8Array()),
        () => crypto.subtle.decrypt({ name: "AES-GCM", iv: new Uint8Array(12) }, key, new Uint8Array()),
        () => crypto.subtle.importKey("raw", new Uint8Array(10), "AES-GCM", false, ["encrypt"]),
        () => crypto.subtle.importKey("raw", new Uint8Array(32), "AES-GCM", false, ["sign"]),
        () => crypto.subtle.importKey("jwk", { kty: "oct", k: btoa("a".repeat(32)).replace(/=/g, ""), ext: false }, "AES-GCM", true, ["encrypt"]),
      ]) { try { await run(); errors.push("accepted"); } catch (error) { errors.push(error.name); } }
      return { errors, opaque: !("keyData" in key) && !("handle" in key), type: key.type, usages: [...key.usages], extractable: key.extractable };
    }
    case "base64": {
      const binary = String.fromCharCode(0, 127, 128, 255);
      const bad = [];
      for (const run of [() => atob("a"), () => atob("a==="), () => btoa("\u0100"), () => atob("YQ=")]) {
        try { run(); bad.push(false); } catch (error) { bad.push(error.name === "InvalidCharacterError"); }
      }
      return { encoded: btoa(binary), decoded: atob(" AH+A/w==\n") === binary, unpadded: atob("YQ") === "a", bad };
    }
    case "clock": {
      class ChildDate extends Date {}
      return { now: Date.now(), constructed: +new Date(), called: Date() === new Date(ctx.now).toString(), explicit: +new Date(1234), utc: Date.UTC(1970, 0, 1), parsed: Date.parse("1970-01-01T00:00:00.000Z"), subclass: +new ChildDate(), instance: new Date() instanceof Date, constructorNow: new Date().constructor.now() };
    }
    case "random": return [Math.random(), Math.random(), Math.random()];
    case "random-denied": { try { Math.random(); return false; } catch (error) { return /intentEntropy/.test(error.message); } }
    case "entropy-denied": { try { crypto.getRandomValues(new Uint8Array(8)); return false; } catch (error) { return error.name === "InvalidAccessError"; } }
    case "action-globals": {
      const bytes = crypto.getRandomValues(new Uint8Array(32));
      return { now: Date.now(), constructed: +new Date(), random: Math.random(), bytes: bytes.length, uuid: /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(crypto.randomUUID()) };
    }
    case "network": return ctx.fetch(vector.url);
    case "serialization": return { value: null, nested: [1, "two", true], omitted: undefined };
    case "cyclic": { const value = {}; value.self = value; return value; }
    default: return vector;
  }
}
