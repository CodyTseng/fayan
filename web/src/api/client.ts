import { bareNostrUser, nostrUserFromEvent } from "@nostr/gadgets/metadata";
import { NostrEvent } from "nostr-tools";
import { npubEncode } from "nostr-tools/nip19";

export function pubkeyToNpub(pubkey: string): string {
  if (pubkey.startsWith("npub1")) {
    return pubkey;
  }
  return npubEncode(pubkey);
}

export interface User {
  pubkey: string;
  npub: string;
  profileEvent?: NostrEvent;
  name: string;
  avatar?: string;
  nip05?: string;
  about?: string;
  rank?: number;
  percentile?: number;
  followersCount?: number;
  followingCount?: number;
}

interface ApiUserResponse {
  event?: NostrEvent;
  pubkey: string;
  rank?: number;
  percentile?: number;
  followers?: number;
  following?: number;
}

function parseUserResponse(data: ApiUserResponse): User {
  const profileEvent =
    data.event && data.event.kind === 0 ? data.event : undefined;
  const profile = profileEvent
    ? nostrUserFromEvent(profileEvent)
    : bareNostrUser(data.pubkey);

  return {
    pubkey: data.pubkey,
    npub: pubkeyToNpub(data.pubkey),
    profileEvent,
    name: profile.shortName,
    avatar: profile.image,
    nip05: profile.metadata.nip05,
    about: profile.metadata.about,
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
  const response = await fetch(`/search?${params}`);
  if (!response.ok) {
    throw new Error(`Search failed: ${response.statusText}`);
  }
  const data: ApiUserResponse[] = await response.json();
  return data.map(parseUserResponse);
}

export async function getUser(pubkeyOrNpub: string): Promise<User | null> {
  const response = await fetch(`/users/${pubkeyOrNpub}`);
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
  const response = await fetch(`/users`, {
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
