import { bareNostrUser, loadNostrUser } from "@nostr/gadgets/metadata";
import { useEffect, useState } from "react";
import { type User, pubkeyToNpub } from "../api/client";

interface UserCardProps {
  user: User;
}

export default function UserCard({ user: _user }: UserCardProps) {
  const [copied, setCopied] = useState(false);
  const [user, setUser] = useState<User>(_user);

  useEffect(() => {
    if (_user.displayName) return;

    const bare = bareNostrUser(_user.pubkey);
    setUser({
      ..._user,
      name: bare.shortName,
      displayName: bare.metadata.display_name,
      picture: bare.image,
      nip05: bare.metadata.nip05,
      about: bare.metadata.about,
    });

    loadNostrUser(_user.pubkey).then((nostrUser) => {
      setUser({
        ..._user,
        name: nostrUser.shortName,
        displayName: nostrUser.metadata.display_name,
        picture: nostrUser.image,
        nip05: nostrUser.metadata.nip05,
        about: nostrUser.metadata.about,
      });
    });
  }, [_user.pubkey]);

  const displayName = user.displayName || user.name || "Unknown";
  const shortPubkey = user.pubkey
    ? `${user.pubkey.slice(0, 8)}...${user.pubkey.slice(-8)}`
    : "";
  const profileUrl = user.pubkey
    ? `https://jumble.social/users/${pubkeyToNpub(user.pubkey)}`
    : "";

  const copyPubkey = async () => {
    if (user.pubkey) {
      await navigator.clipboard.writeText(user.pubkey);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const formatNumber = (num: number | undefined): string => {
    if (num === undefined) return "-";
    if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
    if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
    return num.toString();
  };

  return (
    <div className="py-4 border-b border-gray-100 last:border-0">
      <div className="flex items-start gap-3">
        {/* Avatar */}
        <a
          href={profileUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="flex-shrink-0"
        >
          {user.picture ? (
            <img
              src={user.picture}
              alt={displayName}
              className="w-10 h-10 rounded-full object-cover hover:opacity-80 transition-opacity"
              onError={(e) => {
                (e.target as HTMLImageElement).src =
                  `https://ui-avatars.com/api/?name=${encodeURIComponent(displayName)}&background=f3f4f6&color=374151&size=40`;
              }}
            />
          ) : (
            <div className="w-10 h-10 rounded-full bg-gray-100 flex items-center justify-center text-gray-500 text-sm font-medium hover:bg-gray-200 transition-colors">
              {displayName.charAt(0).toUpperCase()}
            </div>
          )}
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
              {displayName}
            </a>
            {user.nip05 && (
              <span className="text-xs text-gray-400 truncate">
                {user.nip05}
              </span>
            )}
          </div>

          {/* pubkey */}
          <div className="mt-0.5 flex items-center gap-1">
            <code className="text-xs text-gray-400">{shortPubkey}</code>
            <button
              onClick={copyPubkey}
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
        <p className="mt-2 ml-13 text-sm text-gray-500 line-clamp-2">
          {user.about}
        </p>
      )}
    </div>
  );
}
