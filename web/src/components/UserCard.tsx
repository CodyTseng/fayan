import { loadNostrUser } from "@nostr/gadgets/metadata";
import { useEffect, useMemo, useState } from "react";
import { type User } from "../api/client";

interface UserCardProps {
  user: User;
}

export default function UserCard({ user: _user }: UserCardProps) {
  const [copied, setCopied] = useState(false);
  const [user, setUser] = useState<User>(_user);
  const defaultAvatar = useMemo(() => {
    return generateImageByPubkey(user.pubkey);
  }, [user.pubkey]);

  useEffect(() => {
    if (_user.profileEvent) return;

    loadNostrUser(_user.pubkey).then((nostrUser) => {
      setUser({
        ..._user,
        name: nostrUser.shortName,
        avatar: nostrUser.image,
        nip05: nostrUser.metadata.nip05,
        about: nostrUser.metadata.about,
      });
    });
  }, [_user.pubkey]);

  const profileUrl = `https://jumble.social/users/${user.npub}`;

  const copyNpub = async () => {
    await navigator.clipboard.writeText(user.npub);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const formatNumber = (num: number | undefined): string => {
    if (num === undefined) return "-";
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  return (
    <div className="py-4 border-b border-gray-100 last:border-0">
      <div className="flex items-start gap-4">
        {/* Avatar */}
        <a
          href={profileUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="flex-shrink-0"
        >
          <img
            src={user.avatar ?? defaultAvatar}
            alt={user.name}
            className="w-10 h-10 rounded-full object-cover hover:opacity-80 transition-opacity"
            onError={(e) => {
              (e.target as HTMLImageElement).src = defaultAvatar;
            }}
          />
        </a>

        {/* User Info */}
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <a
              href={profileUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="font-medium text-gray-900 truncate hover:underline"
            >
              {user.name}
            </a>
            {user.nip05 && (
              <span className="text-xs text-gray-400 truncate">
                {user.nip05}
              </span>
            )}
          </div>

          {/* pubkey */}
          <div className="mt-0.5 flex items-center gap-1">
            <code className="text-xs text-gray-400 truncate">
              {formatNpub(user.npub)}
            </code>
            <button
              onClick={copyNpub}
              className="text-gray-300 hover:text-gray-500 transition-colors"
            >
              {copied ? (
                <svg
                  className="w-3.5 h-3.5 text-green-500"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M5 13l4 4L19 7"
                  />
                </svg>
              ) : (
                <svg
                  className="w-3.5 h-3.5"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                  />
                </svg>
              )}
            </button>
          </div>

          {/* Stats */}
          <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-gray-500">
            {user.rank !== undefined && (
              <span>
                Rank <span className="text-gray-900">#{user.rank}</span>
              </span>
            )}
            {user.percentile !== undefined && (
              <span>
                Top{" "}
                <span className="text-gray-900">
                  {(100 - user.percentile).toFixed(1)}%
                </span>
              </span>
            )}
            <span>
              Followers{" "}
              <span className="text-gray-900">
                {formatNumber(user.followersCount)}
              </span>
            </span>
            <span>
              Following{" "}
              <span className="text-gray-900">
                {formatNumber(user.followingCount)}
              </span>
            </span>
          </div>
        </div>
      </div>

      {/* About */}
      {user.about && (
        <p className="mt-2 ml-14 text-sm text-gray-500 line-clamp-2">
          {user.about}
        </p>
      )}
    </div>
  );
}

function formatNpub(npub: string): string {
  return npub.slice(0, 10) + "..." + npub.slice(-6);
}

const pubkeyImageCache = new Map<string, string>();
export function generateImageByPubkey(pubkey: string): string {
  if (pubkeyImageCache.has(pubkey)) {
    return pubkeyImageCache.get(pubkey)!;
  }

  const paddedPubkey = pubkey.padEnd(2, "0");

  // Split into 3 parts for colors and the rest for control points
  const colors: string[] = [];
  const controlPoints: string[] = [];
  for (let i = 0; i < 11; i++) {
    const part = paddedPubkey.slice(i * 6, (i + 1) * 6);
    if (i < 3) {
      colors.push(`#${part}`);
    } else {
      controlPoints.push(part);
    }
  }

  // Generate SVG with multiple radial gradients
  const gradients = controlPoints
    .map((point, index) => {
      const cx = parseInt(point.slice(0, 2), 16) % 100;
      const cy = parseInt(point.slice(2, 4), 16) % 100;
      const r = (parseInt(point.slice(4, 6), 16) % 35) + 30;
      const c = colors[index % (colors.length - 1)];

      return `
        <radialGradient id="grad${index}-${pubkey}" cx="${cx}%" cy="${cy}%" r="${r}%">
          <stop offset="0%" style="stop-color:${c};stop-opacity:1" />
          <stop offset="100%" style="stop-color:${c};stop-opacity:0" />
        </radialGradient>
        <rect width="100%" height="100%" fill="url(#grad${index}-${pubkey})" />
      `;
    })
    .join("");

  const image = `
    <svg width="100" height="100" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg">
      <rect width="100%" height="100%" fill="${colors[2]}" fill-opacity="0.3" />
      ${gradients}
    </svg>
  `;
  const imageData = `data:image/svg+xml;base64,${btoa(image)}`;

  pubkeyImageCache.set(pubkey, imageData);

  return imageData;
}
