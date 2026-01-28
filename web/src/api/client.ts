const API_BASE = "https://fayan.jumble.social";

// Bech32 encoding for npub
const ALPHABET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

function bech32Encode(prefix: string, data: Uint8Array): string {
  const values = convertBits(data, 8, 5, true);
  const checksum = createChecksum(prefix, values);
  return (
    prefix + "1" + [...values, ...checksum].map((v) => ALPHABET[v]).join("")
  );
}

function convertBits(
  data: Uint8Array,
  fromBits: number,
  toBits: number,
  pad: boolean,
): number[] {
  let acc = 0;
  let bits = 0;
  const result: number[] = [];
  const maxv = (1 << toBits) - 1;
  for (const value of data) {
    acc = (acc << fromBits) | value;
    bits += fromBits;
    while (bits >= toBits) {
      bits -= toBits;
      result.push((acc >> bits) & maxv);
    }
  }
  if (pad && bits > 0) {
    result.push((acc << (toBits - bits)) & maxv);
  }
  return result;
}

function createChecksum(prefix: string, values: number[]): number[] {
  const enc = [...prefixExpand(prefix), ...values, 0, 0, 0, 0, 0, 0];
  const mod = polymod(enc) ^ 1;
  return [0, 1, 2, 3, 4, 5].map((i) => (mod >> (5 * (5 - i))) & 31);
}

function prefixExpand(prefix: string): number[] {
  const result: number[] = [];
  for (const c of prefix) {
    result.push(c.charCodeAt(0) >> 5);
  }
  result.push(0);
  for (const c of prefix) {
    result.push(c.charCodeAt(0) & 31);
  }
  return result;
}

function polymod(values: number[]): number {
  const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
  let chk = 1;
  for (const v of values) {
    const top = chk >> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ v;
    for (let i = 0; i < 5; i++) {
      if ((top >> i) & 1) chk ^= GEN[i];
    }
  }
  return chk;
}

export function pubkeyToNpub(pubkey: string): string {
  const bytes = new Uint8Array(32);
  for (let i = 0; i < 32; i++) {
    bytes[i] = parseInt(pubkey.slice(i * 2, i * 2 + 2), 16);
  }
  return bech32Encode("npub", bytes);
}

export interface User {
  pubkey: string;
  npub?: string;
  name?: string;
  displayName?: string;
  picture?: string;
  nip05?: string;
  about?: string;
  rank?: number;
  percentile?: number;
  followersCount?: number;
  followingCount?: number;
}

interface ApiUserResponse {
  event?: {
    kind: number;
    id: string;
    pubkey: string;
    created_at: number;
    content: string;
  };
  pubkey: string;
  rank?: number;
  percentile?: number;
  followers?: number;
  following?: number;
}

function parseUserResponse(data: ApiUserResponse): User {
  let profile: Record<string, unknown> = {};
  if (data.event?.content) {
    try {
      profile = JSON.parse(data.event.content);
    } catch {
      // ignore parse error
    }
  }

  return {
    pubkey: data.pubkey,
    name: profile.name as string | undefined,
    displayName: (profile.display_name || profile.displayName) as
      | string
      | undefined,
    picture: profile.picture as string | undefined,
    nip05: profile.nip05 as string | undefined,
    about: profile.about as string | undefined,
    rank: data.rank,
    percentile: data.percentile,
    followersCount: data.followers,
    followingCount: data.following,
  };
}

export async function searchUsers(
  query: string,
  limit: number = 10,
  offset: number = 0,
): Promise<User[]> {
  const params = new URLSearchParams({
    q: query,
    limit: String(limit),
    offset: String(offset),
  });
  const response = await fetch(`${API_BASE}/search?${params}`);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  const data: ApiUserResponse[] = await response.json();
  return data.map(parseUserResponse);
}

export async function getUser(pubkeyOrNpub: string): Promise<User | null> {
  const response = await fetch(`${API_BASE}/users/${pubkeyOrNpub}`);
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(`Failed to get user: ${response.statusText}`);
  }
  const data: ApiUserResponse = await response.json();
  return parseUserResponse(data);
}

export async function getUsers(pubkeys: string[]): Promise<User[]> {
  const response = await fetch(`${API_BASE}/users`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ pubkeys }),
  });
  if (!response.ok) {
    throw new Error(`Failed to get users: ${response.statusText}`);
  }
  const data: Record<string, ApiUserResponse> = await response.json();
  return Object.values(data).map(parseUserResponse);
}
